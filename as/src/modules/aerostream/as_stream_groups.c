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

	if (result != AS_OK) {
		cf_debug(AS_SERVICE,
				"as_stream_groups: commit write failed result %d", result);
	}

	iops_origin_destroy(orig);
	cf_free(orig);
}

/*
 * Build a write cl_msg for consumer_offsets. Three bins: committed, lag,
 * updated_at (all INTEGER). All fields in host byte order for IOPS.
 */
static cl_msg *
build_groups_write_msg(int64_t committed, int64_t lag, int64_t updated_at)
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
	m->info2       = AS_MSG_INFO2_WRITE;
	m->info3       = 0;
	m->info4       = 0;
	m->result_code = 0;
	m->generation  = 0;
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

int64_t
as_stream_groups_get_offset(const char *stream, const char *group,
		uint32_t partition_id)
{
	char key_str[GROUPS_KEY_MAX_LEN];
	int key_len = make_groups_key(key_str, sizeof(key_str),
			stream, group, partition_id);

	if (key_len < 0 || (size_t)key_len >= sizeof(key_str)) {
		cf_warning(AS_SERVICE,
				"as_stream_groups: get_offset key too long stream=%.63s "
				"group=%.63s partition %u",
				stream, group, partition_id);
		return -1;
	}

	as_namespace *ns = as_namespace_get_byname(AS_STREAM_GROUPS_NS);
	if (ns == NULL) {
		return -1;
	}

	cf_digest keyd;
	cf_digest_compute(key_str, (size_t)key_len, &keyd);
	uint32_t pid = as_partition_getid(&keyd);

	as_partition_reservation rsv;
	as_partition_reserve(ns, pid, &rsv);

	as_index_ref r_ref;
	int rv = as_record_get(rsv.tree, &keyd, &r_ref);

	if (rv != 0) {
		as_partition_release(&rsv);
		cf_debug(AS_SERVICE,
				"as_stream_groups: no committed offset for group=%.63s "
				"stream=%.63s partition %u (new group)",
				group, stream, partition_id);
		return -1;
	}

	as_storage_rd rd;
	as_storage_record_open(ns, r_ref.r, &rd);

	int64_t committed = -1;

	/* Small stack array — consumer_offsets records have 3 bins. */
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

	cf_debug(AS_SERVICE,
			"as_stream_groups: commit group=%.63s stream=%.63s partition %u "
			"offset %ld lag %ld",
			group, stream, partition_id, new_offset, lag);

	as_namespace *ns = as_namespace_get_byname(AS_STREAM_GROUPS_NS);
	if (ns == NULL) {
		cf_warning(AS_SERVICE,
				"as_stream_groups: namespace '%s' not found — "
				"commit dropped", AS_STREAM_GROUPS_NS);
		return;
	}

	int64_t now_ns = (int64_t)(cf_clock_getabsolute() * 1000000ULL);

	cf_digest keyd;
	cf_digest_compute(key_str, (size_t)key_len, &keyd);

	cl_msg *write_msgp = build_groups_write_msg(new_offset, lag, now_ns);

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
