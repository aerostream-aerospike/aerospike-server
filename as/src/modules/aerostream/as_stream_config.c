/*
 * as_stream_config.c
 *
 * Per-stream configuration: in-memory cache backed by Aerospike storage.
 *
 * Cache key:   char[AS_STREAM_NAME_SZ] (zero-padded stream name)
 * Storage key: stream_name string (digest computed from it)
 * Storage:     namespace "aerostream", set "config"
 * Bins:        n_parts (INTEGER), ttl_sec (INTEGER), ack_mode (INTEGER)
 *
 * On cache miss, as_stream_config_get falls back to a direct storage read so
 * configs survive server restarts.  Writes are fire-and-forget IOPS.
 */

#include "modules/aerostream/as_stream_config.h"

#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "shash.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/service.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/write.h"

#define AS_STREAM_CONFIG_NS   "aerostream"
#define AS_STREAM_CONFIG_SET  "config"

#define BIN_N_PARTS   "n_parts"
#define BIN_TTL_SEC   "ttl_sec"
#define BIN_ACK_MODE  "ack_mode"

/*
 * Module globals.
 */
static cf_shash *g_config_cache = NULL;

/*
 * Build a fixed-size, zero-padded shash key from a stream name.
 */
static void
make_key(const char *stream_name, char key[AS_STREAM_NAME_SZ])
{
	memset(key, 0, AS_STREAM_NAME_SZ);
	memcpy(key, stream_name, strnlen(stream_name, AS_STREAM_NAME_MAX_LEN));
}

/*
 * done_cb for fire-and-forget IOPS config writes.
 */
static void
config_write_done_cb(void *udata, int result)
{
	iops_origin *orig = (iops_origin *)udata;

	if (result != AS_OK) {
		cf_warning(AS_SERVICE,
				"as_stream_config: storage write failed result %d", result);
	}

	iops_origin_destroy(orig);
	cf_free(orig);
}

/*
 * Build a cl_msg for writing a config record.
 * Three INTEGER bins: n_parts, ttl_sec, ack_mode.
 */
static cl_msg *
build_config_write_msg(const as_stream_config *cfg)
{
	const char *ns_name  = AS_STREAM_CONFIG_NS;
	const char *set_name = AS_STREAM_CONFIG_SET;
	size_t ns_len        = strlen(ns_name);
	size_t set_len       = strlen(set_name);

	uint32_t op_n_parts_sz  = sizeof(uint32_t) + OP_FIXED_SZ + strlen(BIN_N_PARTS)  + sizeof(int64_t);
	uint32_t op_ttl_sec_sz  = sizeof(uint32_t) + OP_FIXED_SZ + strlen(BIN_TTL_SEC)  + sizeof(int64_t);
	uint32_t op_ack_mode_sz = sizeof(uint32_t) + OP_FIXED_SZ + strlen(BIN_ACK_MODE) + sizeof(int64_t);
	uint32_t ops_sz = op_n_parts_sz + op_ttl_sec_sz + op_ack_mode_sz;

	size_t ns_field_sz  = sizeof(uint32_t) + 1 + ns_len;
	size_t set_field_sz = sizeof(uint32_t) + 1 + set_len;
	size_t total_sz     = sizeof(cl_msg) + ns_field_sz + set_field_sz + ops_sz;

	cl_msg *msgp = (cl_msg *)cf_malloc(total_sz);

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type    = PROTO_TYPE_AS_MSG;
	msgp->proto.sz      = total_sz - sizeof(as_proto);

	as_msg *m      = &msgp->msg;
	m->header_sz   = sizeof(as_msg);
	m->info1       = 0;
	m->info2       = AS_MSG_INFO2_WRITE;
	m->info3       = 0;
	m->info4       = 0;
	m->result_code = 0;
	m->generation  = 0;
	m->record_ttl  = 0;  /* config records never expire */
	m->transaction_ttl = 0;
	m->n_fields    = 2;
	m->n_ops       = 3;

	uint8_t *p = (uint8_t *)m->data;

	as_msg_field *mf = (as_msg_field *)p;
	mf->field_sz = (uint32_t)(ns_len + 1);
	mf->type     = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, ns_name, ns_len);
	p += sizeof(uint32_t) + mf->field_sz;

	mf           = (as_msg_field *)p;
	mf->field_sz = (uint32_t)(set_len + 1);
	mf->type     = AS_MSG_FIELD_TYPE_SET;
	memcpy(mf->data, set_name, set_len);
	p += sizeof(uint32_t) + mf->field_sz;

#define WRITE_INT_OP(bname, val) do { \
	uint8_t nlen_ = (uint8_t)strlen(bname); \
	uint32_t osz_ = OP_FIXED_SZ + nlen_ + sizeof(int64_t); \
	memcpy(p, &osz_, sizeof(uint32_t));        p += sizeof(uint32_t); \
	*p++ = AS_MSG_OP_WRITE; \
	*p++ = (uint8_t)AS_PARTICLE_TYPE_INTEGER; \
	*p++ = 0; \
	*p++ = nlen_; \
	memcpy(p, bname, nlen_);                   p += nlen_; \
	int64_t bv_ = (int64_t)cf_swap_to_be64((uint64_t)(val)); \
	memcpy(p, &bv_, sizeof(int64_t));          p += sizeof(int64_t); \
} while (0)

	WRITE_INT_OP(BIN_N_PARTS,  (int64_t)cfg->num_partitions);
	WRITE_INT_OP(BIN_TTL_SEC,  (int64_t)cfg->ttl_seconds);
	WRITE_INT_OP(BIN_ACK_MODE, (int64_t)cfg->ack_mode);

#undef WRITE_INT_OP

	return msgp;
}

/*
 * Read config from storage.  Direct blocking read — called only on cache miss
 * (once per stream per restart).  Safe to call from the service thread for
 * short reads.
 *
 * The config key digest is computed from the stream_name string (same way the
 * write path sets it).
 */
static bool
config_from_storage(const char *stream_name, as_stream_config *out_cfg)
{
	as_namespace *ns = as_namespace_get_byname(AS_STREAM_CONFIG_NS);
	if (ns == NULL) {
		return false;
	}

	cf_digest keyd;
	cf_digest_compute(stream_name, strlen(stream_name), &keyd);

	as_partition_reservation rsv;
	as_partition_reserve(ns, as_partition_getid(&keyd), &rsv);

	as_index_ref r_ref;
	if (as_record_get(rsv.tree, &keyd, &r_ref) != 0) {
		as_partition_release(&rsv);
		return false;
	}

	as_storage_rd rd;
	as_storage_record_open(ns, r_ref.r, &rd);

	bool ok = false;

	/* Small stack array — config records have 3 bins. */
	as_bin stack_bins[8];

	if (as_storage_rd_load_bins(&rd, stack_bins) >= 0) {
		as_bin *n_parts_bin  = as_bin_get(&rd, BIN_N_PARTS);
		as_bin *ttl_sec_bin  = as_bin_get(&rd, BIN_TTL_SEC);
		as_bin *ack_mode_bin = as_bin_get(&rd, BIN_ACK_MODE);

		if (n_parts_bin != NULL) {
			out_cfg->num_partitions = (uint32_t)as_bin_particle_integer_value(n_parts_bin);
			out_cfg->ttl_seconds    = ttl_sec_bin  ?
					(uint32_t)as_bin_particle_integer_value(ttl_sec_bin)  :
					AS_STREAM_DEFAULT_TTL_SECONDS;
			out_cfg->ack_mode       = ack_mode_bin ?
					(uint8_t)as_bin_particle_integer_value(ack_mode_bin)  :
					AS_STREAM_DEFAULT_ACK_MODE;

			if (out_cfg->num_partitions > 0) {
				ok = true;
			}
		}
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);
	as_partition_release(&rsv);

	return ok;
}

/*
 * Public API.
 */

void
as_stream_config_module_init(void)
{
	cf_debug(AS_SERVICE, "as_stream_config: init");

	g_config_cache = cf_shash_create(cf_shash_fn_zstr,
			AS_STREAM_NAME_SZ,
			sizeof(as_stream_config),
			64,
			true);

	if (g_config_cache == NULL) {
		cf_crash(AS_SERVICE, "as_stream_config: failed to create config cache");
	}

	cf_info(AS_SERVICE, "as_stream_config: cache ready");
}

bool
as_stream_config_get(const char *stream_name, as_stream_config *out_cfg)
{
	cf_debug(AS_SERVICE, "as_stream_config: get stream=%.63s", stream_name);

	char key[AS_STREAM_NAME_SZ];
	make_key(stream_name, key);

	/* Fast path: in-memory cache. */
	if (cf_shash_get(g_config_cache, key, out_cfg) == CF_SHASH_OK) {
		cf_debug(AS_SERVICE,
				"as_stream_config: cache hit stream=%.63s "
				"n_parts %u ttl_sec %u ack_mode %u",
				stream_name, out_cfg->num_partitions,
				out_cfg->ttl_seconds, out_cfg->ack_mode);
		return true;
	}

	/* Slow path: storage read on cache miss (once per stream per restart). */
	if (config_from_storage(stream_name, out_cfg)) {
		cf_info(AS_SERVICE,
				"as_stream_config: storage hit stream=%.63s "
				"n_parts %u ttl_sec %u ack_mode %u — populating cache",
				stream_name, out_cfg->num_partitions,
				out_cfg->ttl_seconds, out_cfg->ack_mode);

		cf_shash_put(g_config_cache, key, out_cfg);
		return true;
	}

	cf_debug(AS_SERVICE, "as_stream_config: miss stream=%.63s", stream_name);
	return false;
}

void
as_stream_config_upsert(const char *stream_name, const as_stream_config *cfg)
{
	cf_debug(AS_SERVICE,
			"as_stream_config: upsert stream=%.63s "
			"n_parts %u ttl_sec %u ack_mode %u",
			stream_name, cfg->num_partitions, cfg->ttl_seconds, cfg->ack_mode);

	if (cfg->num_partitions == 0) {
		cf_warning(AS_SERVICE,
				"as_stream_config: upsert stream=%.63s rejected — "
				"num_partitions cannot be 0",
				stream_name);
		return;
	}

	/* Update in-memory cache. */
	char key[AS_STREAM_NAME_SZ];
	make_key(stream_name, key);
	cf_shash_put(g_config_cache, key, cfg);

	cf_debug(AS_SERVICE,
			"as_stream_config: cache updated stream=%.63s cache_size %u",
			stream_name, cf_shash_get_size(g_config_cache));

	/* Persist to storage — fire-and-forget IOPS write. */
	as_namespace *ns = as_namespace_get_byname(AS_STREAM_CONFIG_NS);
	if (ns == NULL) {
		cf_debug(AS_SERVICE,
				"as_stream_config: namespace '%s' not found — "
				"config not persisted for stream=%.63s",
				AS_STREAM_CONFIG_NS, stream_name);
		return;
	}

	cf_digest keyd;
	cf_digest_compute(stream_name, strlen(stream_name), &keyd);

	cl_msg *write_msgp = build_config_write_msg(cfg);

	iops_origin *orig = (iops_origin *)cf_malloc(sizeof(iops_origin));
	orig->msgp       = write_msgp;
	orig->filter_exp = NULL;
	orig->expops     = NULL;
	orig->check_cb   = NULL;
	orig->done_cb    = config_write_done_cb;
	orig->udata      = orig;

	as_transaction tr;
	as_transaction_init_iops(&tr, NULL, &keyd, orig);
	as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_SET);

	as_service_enqueue_internal(&tr);

	cf_debug(AS_SERVICE,
			"as_stream_config: storage write enqueued stream=%.63s",
			stream_name);
}

void
as_stream_config_get_or_default(const char *stream_name,
		as_stream_config *out_cfg)
{
	cf_debug(AS_SERVICE,
			"as_stream_config: get_or_default stream=%.63s", stream_name);

	if (as_stream_config_get(stream_name, out_cfg)) {
		return;
	}

	out_cfg->num_partitions = AS_STREAM_DEFAULT_NUM_PARTITIONS;
	out_cfg->ttl_seconds    = AS_STREAM_DEFAULT_TTL_SECONDS;
	out_cfg->ack_mode       = AS_STREAM_DEFAULT_ACK_MODE;

	cf_info(AS_SERVICE,
			"as_stream_config: new stream=%.63s applying defaults "
			"n_parts %u ttl_sec %u ack_mode %u",
			stream_name, out_cfg->num_partitions,
			out_cfg->ttl_seconds, out_cfg->ack_mode);

	as_stream_config_upsert(stream_name, out_cfg);
}
