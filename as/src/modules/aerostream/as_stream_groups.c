/*
 * as_stream_groups.c
 *
 * Consumer group offset persistence.
 */

#include "modules/aerostream/as_stream_groups.h"
#include "modules/aerostream/as_stream_config.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "socket.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/service.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/write.h"

/* Key: "{group}:{stream}:{partition_id}" — lengths 63+1+63+1+10 */
#define GROUPS_KEY_MAX_LEN  140

/* Bin names in the consumer_offsets set. */
#define BIN_COMMITTED   "committed"
#define BIN_LAG         "lag"
#define BIN_UPDATED_AT  "updated_at"

/*
 * Build key string for a (group, stream, partition) triple.
 */
static int
make_groups_key(char *buf, size_t buf_sz, const char *stream,
		const char *group, uint32_t partition_id)
{
	return snprintf(buf, buf_sz, "%.63s:%.63s:%u",
			group, stream, partition_id);
}

/*
 * done_cb for fire-and-forget IOPS commit writes.
 * Just frees the iops_origin msgp.
 */
static void
commit_done_cb(void *udata, int result)
{
	iops_origin *orig = (iops_origin *)udata;

	if (result == AS_ERR_GENERATION || result == AS_ERR_RECORD_EXISTS) {
		/* Expected, benign: a concurrent consumer in the same group won the
		 * CAS. Dropping this write is exactly what prevents double-commit. */
		cf_debug(AS_SERVICE,
				"as_stream_groups: commit lost CAS race (result %d) — dropped",
				result);
	}
	else if (result != AS_OK) {
		cf_warning(AS_SERVICE,
				"as_stream_groups: commit write failed result %d", result);
	}

	iops_origin_destroy(orig);
	cf_free(orig);
}

/*
 * Build a write cl_msg for consumer_offsets. Three bins: committed, lag,
 * updated_at (all INTEGER). All fields in host byte order for IOPS.
 *
 * extra_info2 / generation drive optimistic concurrency (CAS):
 *   - AS_MSG_INFO2_GENERATION + generation = G : write only if the record's
 *     current generation is still G (rejects concurrent double-commits).
 *   - AS_MSG_INFO2_CREATE_ONLY : write only if the record does not yet exist.
 */
static cl_msg *
build_groups_write_msg(int64_t committed, int64_t lag, int64_t updated_at,
		uint8_t extra_info2, uint16_t generation)
{
	const char *ns_name  = AS_STREAM_GROUPS_NS;
	const char *set_name = AS_STREAM_GROUPS_SET;

	size_t ns_name_len  = strlen(ns_name);
	size_t set_name_len = strlen(set_name);

	/* Each INTEGER op: 4(op_sz) + 4(OP_FIXED_SZ) + name_len + 8(int64) */
	uint32_t op_committed_sz = sizeof(uint32_t) + OP_FIXED_SZ
			+ strlen(BIN_COMMITTED) + sizeof(int64_t);
	uint32_t op_lag_sz = sizeof(uint32_t) + OP_FIXED_SZ
			+ strlen(BIN_LAG) + sizeof(int64_t);
	uint32_t op_updated_sz = sizeof(uint32_t) + OP_FIXED_SZ
			+ strlen(BIN_UPDATED_AT) + sizeof(int64_t);
	uint32_t ops_sz = op_committed_sz + op_lag_sz + op_updated_sz;

	size_t ns_field_sz  = sizeof(uint32_t) + 1 + ns_name_len;
	size_t set_field_sz = sizeof(uint32_t) + 1 + set_name_len;
	size_t total_sz     = sizeof(cl_msg) + ns_field_sz + set_field_sz + ops_sz;

	cl_msg *msgp = (cl_msg *)cf_malloc(total_sz);

	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type    = PROTO_TYPE_AS_MSG;
	msgp->proto.sz      = total_sz - sizeof(as_proto);

	as_msg *m      = &msgp->msg;
	m->header_sz   = sizeof(as_msg);
	m->info1       = 0;
	m->info2       = AS_MSG_INFO2_WRITE | extra_info2;
	m->info3       = 0;
	m->info4       = 0;
	m->result_code = 0;
	m->generation  = generation;  /* checked iff info2 has GENERATION */
	m->record_ttl  = 0;  /* never expire — group offsets are permanent */
	m->transaction_ttl = 0;
	m->n_fields    = 2;
	m->n_ops       = 3;

	uint8_t *p = (uint8_t *)m->data;

	/* Namespace field */
	as_msg_field *mf = (as_msg_field *)p;
	mf->field_sz = (uint32_t)(ns_name_len + 1);
	mf->type     = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, ns_name, ns_name_len);
	p += sizeof(uint32_t) + mf->field_sz;

	/* Set field */
	mf           = (as_msg_field *)p;
	mf->field_sz = (uint32_t)(set_name_len + 1);
	mf->type     = AS_MSG_FIELD_TYPE_SET;
	memcpy(mf->data, set_name, set_name_len);
	p += sizeof(uint32_t) + mf->field_sz;

	/* Helper: write one INTEGER op inline */
#define WRITE_INT_OP(bin_name, val) do { \
	uint8_t name_len_ = (uint8_t)strlen(bin_name); \
	uint32_t op_sz_   = OP_FIXED_SZ + name_len_ + sizeof(int64_t); \
	memcpy(p, &op_sz_, sizeof(uint32_t));          p += sizeof(uint32_t); \
	*p++ = AS_MSG_OP_WRITE; \
	*p++ = (uint8_t)AS_PARTICLE_TYPE_INTEGER; \
	*p++ = 0; /* flags */ \
	*p++ = name_len_; \
	memcpy(p, bin_name, name_len_);                p += name_len_; \
	int64_t be_ = (int64_t)cf_swap_to_be64((uint64_t)(val)); \
	memcpy(p, &be_, sizeof(int64_t));              p += sizeof(int64_t); \
} while (0)

	WRITE_INT_OP(BIN_COMMITTED,  committed);
	WRITE_INT_OP(BIN_LAG,        lag);
	WRITE_INT_OP(BIN_UPDATED_AT, updated_at);

#undef WRITE_INT_OP

	return msgp;
}

/*
 * Public API.
 */

void
as_stream_groups_module_init(void)
{
	cf_info(AS_SERVICE, "as_stream_groups: init");
}

/*
 * Read the current committed offset, record generation, and existence for a
 * (group, stream, partition). Returns committed (or -1 if absent). *out_gen and
 * *out_exists describe the record for the CAS write that follows.
 */
static int64_t
read_committed_gen(const char *stream, const char *group, uint32_t partition_id,
		uint16_t *out_gen, bool *out_exists)
{
	*out_gen = 0;
	*out_exists = false;

	char key_str[GROUPS_KEY_MAX_LEN];
	int key_len = make_groups_key(key_str, sizeof(key_str),
			stream, group, partition_id);

	if (key_len < 0 || (size_t)key_len >= sizeof(key_str)) {
		cf_warning(AS_SERVICE,
				"as_stream_groups: key too long stream=%.63s group=%.63s "
				"partition %u",
				stream, group, partition_id);
		return -1;
	}

	as_namespace *ns = as_namespace_get_byname(AS_STREAM_GROUPS_NS);
	if (ns == NULL) {
		return -1;
	}

	cf_digest keyd;
	cf_digest_compute(key_str, (size_t)key_len, &keyd);

	as_partition_reservation rsv;
	as_partition_reserve(ns, as_partition_getid(&keyd), &rsv);

	as_index_ref r_ref;
	if (as_record_get(rsv.tree, &keyd, &r_ref) != 0) {
		as_partition_release(&rsv);
		return -1;  /* new group, no record yet */
	}

	*out_exists = true;
	*out_gen = (uint16_t)r_ref.r->generation;

	as_storage_rd rd;
	as_storage_record_open(ns, r_ref.r, &rd);

	int64_t committed = -1;
	as_bin stack_bins[8];

	if (as_storage_rd_load_bins(&rd, stack_bins) >= 0) {
		as_bin *b = as_bin_get(&rd, BIN_COMMITTED);
		if (b != NULL) {
			committed = as_bin_particle_integer_value(b);
		}
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);
	as_partition_release(&rsv);

	return committed;
}

int64_t
as_stream_groups_get_offset(const char *stream, const char *group,
		uint32_t partition_id)
{
	uint16_t gen;
	bool exists;
	int64_t committed = read_committed_gen(stream, group, partition_id,
			&gen, &exists);

	cf_debug(AS_SERVICE,
			"as_stream_groups: get_offset group=%.63s stream=%.63s "
			"partition %u committed %ld",
			group, stream, partition_id, committed);

	return committed;
}

void
as_stream_groups_commit(const char *stream, const char *group,
		uint32_t partition_id, int64_t new_offset, int64_t lag)
{
	char key_str[GROUPS_KEY_MAX_LEN];
	int key_len = make_groups_key(key_str, sizeof(key_str),
			stream, group, partition_id);

	if (key_len < 0 || (size_t)key_len >= sizeof(key_str)) {
		cf_warning(AS_SERVICE,
				"as_stream_groups: commit key too long stream=%.63s "
				"group=%.63s partition %u",
				stream, group, partition_id);
		return;
	}

	as_namespace *ns = as_namespace_get_byname(AS_STREAM_GROUPS_NS);
	if (ns == NULL) {
		cf_warning(AS_SERVICE,
				"as_stream_groups: namespace '%s' not found — "
				"commit dropped", AS_STREAM_GROUPS_NS);
		return;
	}

	/*
	 * CAS: read the current committed offset + record generation, then write
	 * conditionally. This is optimistic concurrency, not a lock — two consumers
	 * in the same group racing to commit will both read the same generation,
	 * but only the first gen-checked write lands; the loser's write fails with
	 * AS_ERR_GENERATION and is dropped. That prevents the split-brain
	 * double-commit the plain write allowed.
	 */
	uint16_t gen;
	bool exists;
	int64_t committed = read_committed_gen(stream, group, partition_id,
			&gen, &exists);

	/* Stale or out-of-order ack: never move the committed offset backwards. */
	if (exists && new_offset <= committed) {
		cf_debug(AS_SERVICE,
				"as_stream_groups: commit skipped (stale) group=%.63s "
				"stream=%.63s partition %u new %ld <= committed %ld",
				group, stream, partition_id, new_offset, committed);
		return;
	}

	uint8_t  extra_info2 = exists ? AS_MSG_INFO2_GENERATION : AS_MSG_INFO2_CREATE_ONLY;
	uint16_t use_gen     = exists ? gen : 0;

	cf_debug(AS_SERVICE,
			"as_stream_groups: commit group=%.63s stream=%.63s partition %u "
			"offset %ld lag %ld (%s gen %u)",
			group, stream, partition_id, new_offset, lag,
			exists ? "CAS" : "create", use_gen);

	int64_t now_ns = (int64_t)(cf_clock_getabsolute() * 1000000ULL);

	cf_digest keyd;
	cf_digest_compute(key_str, (size_t)key_len, &keyd);

	cl_msg *write_msgp = build_groups_write_msg(new_offset, lag, now_ns,
			extra_info2, use_gen);

	iops_origin *orig = (iops_origin *)cf_malloc(sizeof(iops_origin));
	orig->msgp       = write_msgp;
	orig->filter_exp = NULL;
	orig->expops     = NULL;
	orig->check_cb   = NULL;
	orig->done_cb    = commit_done_cb;
	orig->udata      = orig;  /* done_cb receives orig itself to free it */

	as_transaction tr;
	as_transaction_init_iops(&tr, NULL, &keyd, orig);
	as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_SET);

	as_service_enqueue_internal(&tr);
}
