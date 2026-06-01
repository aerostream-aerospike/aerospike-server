/*
 * as_stream_log.c
 *
 * Produce path for AeroStream.
 *
 * Offset assignment is a single lock-free atomic increment (as_faa_uint64).
 * The storage write is an async IOPS transaction. For ack_mode > 0 the
 * done_cb sends the PROD_ACK and rearms the fd. For ack_mode == 0 the fd
 * is rearmed immediately and the write is fire-and-forget.
 */

#include "modules/aerostream/as_stream_log.h"
#include "modules/aerostream/as_stream_config.h"
#include "modules/aerostream/as_stream_groups.h"
#include "modules/aerostream/as_stream_pubsub.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>

#include "citrusleaf/cf_clock.h"  /* brings in <time.h> + CLOCK_REALTIME */

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "log.h"
#include "shash.h"
#include "socket.h"

#include "cf_thread.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/service.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/write.h"

/*
 * Namespace and set that hold log records.
 */
#define AS_STREAM_NS    "aerostream"
#define AS_STREAM_SET   "log"

/*
 * Maximum key string length: stream(63) + ":" + partition(10) + ":" + offset(20)
 */
#define LOG_KEY_MAX_LEN  96

/*
 * Stack bin array size for storage reads. AeroStream records have at most a
 * handful of bins; this avoids the multi-megabyte RECORD_MAX_BINS VLA in the
 * per-record push loop.
 */
#define AS_STREAM_MAX_READ_BINS  8

/*
 * Per-connection send lock. Shared across partition sessions when a consumer
 * subscribes to all partitions (partition_id = 0xFFFFFFFF) so concurrent push
 * loop threads serialise their writes to the same fd_h.
 */
typedef struct {
	cf_mutex  mtx;
	uint32_t  ref_count;  /* number of sessions sharing this lock */
} as_stream_fd_lock;

/*
 * One session per (consumer, partition). Stored as a singly-linked list on
 * the partition under push_lock. Freed by the push loop when active = false.
 */
typedef struct as_stream_session_s {
	as_file_handle          *fd_h;
	as_stream_fd_lock       *send_lock;     /* serialises writes to fd_h */
	uint64_t                 correlation_id; /* echoed in STREAM_RECORD (BE) */
	char                     group[AS_STREAM_NAME_SZ];
	char                     stream[AS_STREAM_NAME_SZ];
	uint32_t                 partition_id;
	int64_t                  next_offset;   /* under push_lock */
	uint32_t                 max_in_flight;
	uint32_t                 in_flight;     /* under push_lock */
	bool                     active;        /* false = push loop will free */
	struct as_stream_session_s *next;
} as_stream_session;

/*
 * Per-partition in-memory state.
 */
typedef struct {
	uint64_t          offset_seq;   /* atomic; as_faa_uint64 returns old = offset */
	cf_mutex          push_lock;    /* guards sessions list, next_offset, in_flight */
	cf_condition      push_cond;    /* wakes push loop: new produce or new ACK */
	as_stream_session *sessions;    /* singly-linked list under push_lock */
	cf_tid            push_tid;     /* push loop thread id */
} as_stream_partition;

/*
 * Per-stream in-memory state.  Variable-size (flexible array at end).
 * Always heap-allocated; stored as a pointer in g_stream_states.
 */
typedef struct {
	char                 name[AS_STREAM_NAME_SZ];
	uint32_t             num_partitions;
	as_stream_partition  partitions[];
} as_stream_state;

/*
 * Module globals.
 */

/* key = char[AS_STREAM_NAME_SZ], value = as_stream_state* */
static cf_shash  *g_stream_states;
/* protects stream state creation (not offset_seq updates) */
static cf_mutex   g_stream_lock;

/*
 * Context heap-allocated per PRODUCE, freed in done_cb.
 */
typedef struct {
	as_file_handle  *fd_h;
	uint64_t         correlation_id;  /* wire byte order (big-endian) */
	int64_t          offset;          /* host order */
	uint32_t         partition_id;    /* host order */
	uint64_t         timestamp_ns;    /* host order */
	uint8_t          ack_mode;
	iops_origin      iops_orig;       /* embedded; owns msgp */
} as_stream_produce_ctx;

/*
 * Helpers.
 */

static inline uint64_t
wall_clock_ns(void)
{
	/*
	 * cf_clock_getabsolute() returns CLOCK_REALTIME in milliseconds.
	 * Multiply to nanoseconds for consistent ts bin precision across all
	 * callers. Phase-3 limitation: millisecond precision only.
	 * TODO: call clock_gettime(CLOCK_REALTIME) directly once build flags
	 * are confirmed in the IDE environment.
	 */
	return cf_clock_getabsolute() * 1000000ULL;
}

/* FNV-32a over arbitrary bytes. */
static inline uint32_t
fnv32a(const uint8_t *data, uint32_t len)
{
	uint32_t h = 2166136261u;
	for (uint32_t i = 0; i < len; i++) {
		h = (h ^ data[i]) * 16777619u;
	}
	return h;
}

/*
 * Encode helpers write the as_msg_op wire format byte-by-byte to avoid any
 * struct-cast or bitfield-address issues with __attribute__((packed)) structs.
 *
 * as_msg_op layout (8 fixed bytes):
 *   [0-3]  op_sz        uint32 (host order, does not include itself)
 *   [4]    op           uint8
 *   [5]    particle_type uint8
 *   [6]    flags byte   uint8  (has_lut:1 | unused_flags:7, always 0 here)
 *   [7]    name_sz      uint8
 *   [8..8+name_sz-1]    bin name bytes
 *   [8+name_sz..]       particle data
 */

/* Write a WRITE INTEGER op into buf, return bytes consumed. */
static uint32_t
encode_int_op(uint8_t *buf, const char *bin_name, int64_t value)
{
	uint8_t  name_len = (uint8_t)strlen(bin_name);
	uint32_t op_sz    = OP_FIXED_SZ + name_len + sizeof(int64_t);
	uint8_t *p        = buf;

	memcpy(p, &op_sz, sizeof(uint32_t));   p += sizeof(uint32_t);
	*p++ = AS_MSG_OP_WRITE;
	*p++ = (uint8_t)AS_PARTICLE_TYPE_INTEGER;
	*p++ = 0;                               /* flags byte: has_lut=0, unused=0 */
	*p++ = name_len;
	memcpy(p, bin_name, name_len);          p += name_len;

	/* INTEGER particles are big-endian on the wire. */
	int64_t be_val = (int64_t)cf_swap_to_be64((uint64_t)value);
	memcpy(p, &be_val, sizeof(int64_t));

	return sizeof(uint32_t) + op_sz;
}

/* Write a WRITE BLOB op into buf, return bytes consumed. */
static uint32_t
encode_blob_op(uint8_t *buf, const char *bin_name,
		const uint8_t *data, uint32_t data_sz)
{
	uint8_t  name_len = (uint8_t)strlen(bin_name);
	uint32_t op_sz    = OP_FIXED_SZ + name_len + data_sz;
	uint8_t *p        = buf;

	memcpy(p, &op_sz, sizeof(uint32_t));   p += sizeof(uint32_t);
	*p++ = AS_MSG_OP_WRITE;
	*p++ = (uint8_t)AS_PARTICLE_TYPE_BLOB;
	*p++ = 0;                               /* flags byte */
	*p++ = name_len;
	memcpy(p, bin_name, name_len);          p += name_len;
	if (data_sz > 0) {
		memcpy(p, data, data_sz);
	}

	return sizeof(uint32_t) + op_sz;
}

/*
 * Build a cl_msg for an internal write to namespace "aerostream", set "log".
 * Writes three bins: payload (BLOB), ts (INTEGER), offset (INTEGER).
 *
 * Caller must cf_free() the returned msgp.
 *
 * All integer fields in host byte order (IOPS path; as_transaction_prepare
 * is not called, so no server-side byte swap).
 * INTEGER particle values are big-endian (as_particle_from_wire expectation).
 */
static cl_msg *
build_log_write_msg(uint32_t record_ttl,
		const uint8_t *payload, uint32_t payload_sz,
		int64_t ts_ns, int64_t offset)
{
	/* Sizes of each op in the buffer. */
	uint32_t op_payload_sz = sizeof(uint32_t) + OP_FIXED_SZ
			+ strlen("payload") + payload_sz;
	uint32_t op_ts_sz      = sizeof(uint32_t) + OP_FIXED_SZ
			+ strlen("ts") + sizeof(int64_t);
	uint32_t op_offset_sz  = sizeof(uint32_t) + OP_FIXED_SZ
			+ strlen("offset") + sizeof(int64_t);
	uint32_t ops_sz = op_payload_sz + op_ts_sz + op_offset_sz;

	/* Field sizes (4-byte field_sz + 1-byte type + name bytes). */
	size_t ns_name_len  = strlen(AS_STREAM_NS);
	size_t set_name_len = strlen(AS_STREAM_SET);
	size_t ns_field_sz  = sizeof(uint32_t) + 1 + ns_name_len;
	size_t set_field_sz = sizeof(uint32_t) + 1 + set_name_len;

	size_t total_sz = sizeof(cl_msg) + ns_field_sz + set_field_sz + ops_sz;
	cl_msg *msgp = (cl_msg *)cf_malloc(total_sz);

	/* as_proto header. */
	msgp->proto.version = PROTO_VERSION;
	msgp->proto.type    = PROTO_TYPE_AS_MSG;
	msgp->proto.sz      = total_sz - sizeof(as_proto);

	/* as_msg header — host byte order for IOPS (no swap by server). */
	as_msg *m          = &msgp->msg;
	m->header_sz       = sizeof(as_msg);
	m->info1           = 0;
	m->info2           = AS_MSG_INFO2_WRITE;
	m->info3           = 0;
	m->info4           = 0;
	m->result_code     = 0;
	m->generation      = 0;
	m->record_ttl      = record_ttl;
	m->transaction_ttl = 0;
	m->n_fields        = 2;  /* namespace + set */
	m->n_ops           = 3;  /* payload, ts, offset */

	uint8_t *p = (uint8_t *)m->data;

	/* Namespace field. */
	as_msg_field *mf  = (as_msg_field *)p;
	mf->field_sz      = (uint32_t)(ns_name_len + 1);  /* +1 for type byte */
	mf->type          = AS_MSG_FIELD_TYPE_NAMESPACE;
	memcpy(mf->data, AS_STREAM_NS, ns_name_len);
	p += sizeof(uint32_t) + mf->field_sz;

	/* Set field. */
	mf               = (as_msg_field *)p;
	mf->field_sz     = (uint32_t)(set_name_len + 1);
	mf->type         = AS_MSG_FIELD_TYPE_SET;
	memcpy(mf->data, AS_STREAM_SET, set_name_len);
	p += sizeof(uint32_t) + mf->field_sz;

	/* Ops. */
	p += encode_blob_op(p, "payload", payload, payload_sz);
	p += encode_int_op(p, "ts",     ts_ns);
	p += encode_int_op(p, "offset", offset);

	return msgp;
}

/*
 * Read one log record from storage.  Called from the push loop with no locks
 * held.  Returns true and fills out_* if found; caller must cf_free(*out_payload).
 * Returns false if the record does not exist yet.
 */
static bool
push_read_record(as_namespace *ns, const char *stream_name,
		uint32_t partition_id, int64_t offset,
		uint8_t **out_payload, uint32_t *out_payload_sz, int64_t *out_ts_ns)
{
	char key_str[LOG_KEY_MAX_LEN];
	int  key_len = snprintf(key_str, sizeof(key_str), "%.63s:%u:%ld",
			stream_name, partition_id, offset);

	if (key_len < 0 || (size_t)key_len >= sizeof(key_str)) {
		return false;
	}

	cf_digest keyd;
	cf_digest_compute(key_str, (size_t)key_len, &keyd);

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

	/*
	 * Load bins into a caller-provided stack array. AeroStream log records
	 * have at most 4 bins (payload, ts, offset, hdrs), so a small fixed array
	 * is safe — no need for the multi-megabyte RECORD_MAX_BINS array.
	 */
	as_bin stack_bins[AS_STREAM_MAX_READ_BINS];

	if (as_storage_rd_load_bins(&rd, stack_bins) >= 0) {
		as_bin *ts_bin  = as_bin_get(&rd, "ts");
		as_bin *pay_bin = as_bin_get(&rd, "payload");

		if (ts_bin != NULL) {
			*out_ts_ns = as_bin_particle_integer_value(ts_bin);
		}

		if (pay_bin != NULL) {
			uint8_t *raw;
			*out_payload_sz = as_bin_particle_blob_ptr(pay_bin, &raw);
			*out_payload    = (uint8_t *)cf_malloc(*out_payload_sz);
			memcpy(*out_payload, raw, *out_payload_sz);
		}
		else {
			*out_payload    = NULL;
			*out_payload_sz = 0;
		}

		ok = true;
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);
	as_partition_release(&rsv);

	return ok;
}

/*
 * Build and send a STREAM_RECORD message on fd_h.
 * All numeric fields in big-endian on the wire.
 * Returns true on success, false if the socket write failed.
 * Non-static so as_stream_pubsub.c can use it for fan-out.
 */
bool
as_stream_log_send_record(as_file_handle *fd_h, uint64_t correlation_id,
		uint32_t partition_id, int64_t offset, int64_t ts_ns,
		const uint8_t *payload, uint32_t payload_sz)
{
	/* 8(corr) + 4(pid) + 8(offset) + 8(ts) + 2(hdr_count) + 4(pay_sz) = 34 */
	size_t body_sz  = sizeof(as_stream_record_msg) + payload_sz;
	size_t total_sz = sizeof(as_proto) + body_sz;
	uint8_t *buf    = (uint8_t *)cf_malloc(total_sz);
	uint8_t *p      = buf;

	as_proto proto_hdr;
	proto_hdr.version = PROTO_VERSION;
	proto_hdr.type    = AS_PROTO_TYPE_STREAM_RECORD;
	proto_hdr.sz      = body_sz;
	as_proto_swap(&proto_hdr);
	memcpy(p, &proto_hdr, sizeof(as_proto));     p += sizeof(as_proto);

	uint64_t be_corr = cf_swap_to_be64(correlation_id);
	memcpy(p, &be_corr, 8);                      p += 8;

	uint32_t be_pid = cf_swap_to_be32(partition_id);
	memcpy(p, &be_pid, 4);                       p += 4;

	int64_t be_off = (int64_t)cf_swap_to_be64((uint64_t)offset);
	memcpy(p, &be_off, 8);                       p += 8;

	uint64_t be_ts = cf_swap_to_be64((uint64_t)ts_ns);
	memcpy(p, &be_ts, 8);                        p += 8;

	uint16_t be_hdr = cf_swap_to_be16(0);        /* headers_count = 0 */
	memcpy(p, &be_hdr, 2);                       p += 2;

	uint32_t be_pay = cf_swap_to_be32(payload_sz);
	memcpy(p, &be_pay, 4);                       p += 4;

	if (payload_sz > 0) {
		memcpy(p, payload, payload_sz);
	}

	bool ok = (cf_socket_send_all(&fd_h->sock, buf, total_sz,
			MSG_NOSIGNAL, CF_SOCKET_TIMEOUT) >= 0);

	cf_free(buf);
	return ok;
}

/*
 * Args passed to push_loop_thread at spawn.
 */
typedef struct {
	as_stream_state *state;
	uint32_t         partition_id;
} push_thread_args;

/*
 * Push loop: one thread per (stream, partition), spawned when the stream is
 * first created.  Delivers STREAM_RECORD messages to all active sessions.
 *
 * Locking discipline:
 *   push_lock  — held while reading/writing session state.
 *   send_lock  — acquired AFTER releasing push_lock; serialises fd_h writes
 *                when a consumer subscribes to all partitions (0xFFFFFFFF).
 *   Never hold both simultaneously.
 */
static void *
push_loop_thread(void *udata)
{
	push_thread_args    *args  = (push_thread_args *)udata;
	as_stream_state     *state = args->state;
	uint32_t             pid   = args->partition_id;
	cf_free(args);

	as_stream_partition *part = &state->partitions[pid];

	cf_info(AS_SERVICE,
			"as_stream_log: push loop start stream=%.63s partition %u",
			state->name, pid);

	cf_mutex_lock(&part->push_lock);

	while (true) {
		/* Re-resolve namespace each pass so a delayed config still works. */
		as_namespace *ns = as_namespace_get_byname(AS_STREAM_NS);

		if (ns == NULL) {
			cf_debug(AS_SERVICE,
					"as_stream_log: push loop namespace '%s' not found, "
					"waiting stream=%.63s partition %u",
					AS_STREAM_NS, state->name, pid);
			cf_condition_wait(&part->push_cond, &part->push_lock);
			continue;
		}

		bool did_work   = false;
		int64_t head    = (int64_t)as_load_uint64(&part->offset_seq) - 1;

		as_stream_session *prev = NULL;
		as_stream_session *sess = part->sessions;

		while (sess != NULL) {
			/* Reap sessions that died (send failure or UNSUB). */
			if (!sess->active) {
				as_stream_session *dead = sess;

				if (prev != NULL) prev->next = sess->next;
				else              part->sessions = sess->next;

				sess = sess->next;

				if (--dead->send_lock->ref_count == 0) {
					cf_free(dead->send_lock);
				}

				cf_free(dead);

				cf_debug(AS_SERVICE,
						"as_stream_log: reaped dead session "
						"stream=%.63s partition %u",
						state->name, pid);
				continue;
			}

			/* Push records until in_flight is full or head is reached. */
			while (sess->in_flight < sess->max_in_flight
					&& sess->next_offset <= head) {

				int64_t            read_off = sess->next_offset;
				uint64_t           corr_id  = sess->correlation_id;
				uint32_t           part_id  = sess->partition_id;
				as_file_handle    *fd_h     = sess->fd_h;
				as_stream_fd_lock *sl       = sess->send_lock;

				cf_mutex_unlock(&part->push_lock);

				/* Storage read — blocking, no lock held. */
				uint8_t *payload    = NULL;
				uint32_t payload_sz = 0;
				int64_t  ts_ns      = 0;
				bool     found      = push_read_record(ns, state->name, pid,
						read_off, &payload, &payload_sz, &ts_ns);

				bool sent = false;

				if (found) {
					cf_debug(AS_SERVICE,
							"as_stream_log: push record "
							"stream=%.63s partition %u offset %ld "
							"payload_sz %u fd %d",
							state->name, pid, read_off,
							payload_sz, CSFD(&fd_h->sock));

					/* Serialise write to fd_h across partition threads. */
					cf_mutex_lock(&sl->mtx);
					sent = as_stream_log_send_record(fd_h, corr_id, part_id,
							read_off, ts_ns, payload, payload_sz);
					cf_mutex_unlock(&sl->mtx);

					cf_free(payload);
				}

				cf_mutex_lock(&part->push_lock);

				/* Session may have been killed while lock was released. */
				if (!sess->active) {
					break;
				}

				if (found && sent) {
					sess->next_offset++;
					sess->in_flight++;
					did_work = true;
					head = (int64_t)as_load_uint64(&part->offset_seq) - 1;
				}
				else if (found && !sent) {
					cf_debug(AS_SERVICE,
							"as_stream_log: send failed, killing session "
							"stream=%.63s partition %u fd %d",
							state->name, pid, CSFD(&fd_h->sock));
					sess->active = false;
					break;
				}
				else {
					/* Record not in storage yet — wait for next signal. */
					break;
				}
			}

			prev = sess;
			sess = sess->next;
		}

		if (!did_work) {
			cf_condition_wait(&part->push_cond, &part->push_lock);
		}
	}

	/* Unreachable — threads run for the lifetime of the server. */
	cf_mutex_unlock(&part->push_lock);
	return NULL;
}

/*
 * Check if a log record at the given offset exists in storage.
 * Used only during startup offset reconstruction — O(1) direct key read.
 */
static bool
log_record_exists(as_namespace *ns, const char *stream_name,
		uint32_t partition_id, int64_t offset)
{
	char key_str[LOG_KEY_MAX_LEN];
	int  key_len = snprintf(key_str, sizeof(key_str), "%.63s:%u:%ld",
			stream_name, partition_id, offset);

	if (key_len < 0 || (size_t)key_len >= sizeof(key_str)) {
		return false;
	}

	cf_digest keyd;
	cf_digest_compute(key_str, (size_t)key_len, &keyd);

	as_partition_reservation rsv;
	as_partition_reserve(ns, as_partition_getid(&keyd), &rsv);

	as_index_ref r_ref;
	bool exists = (as_record_get(rsv.tree, &keyd, &r_ref) == 0);

	if (exists) {
		as_record_done(&r_ref, ns);
	}

	as_partition_release(&rsv);
	return exists;
}

/*
 * Find the maximum committed offset for (stream, partition) using exponential
 * search to locate the upper bound, then binary search to narrow it down.
 * O(log N) direct key reads.  Returns -1 if no records exist.
 */
static int64_t
find_max_offset(as_namespace *ns, const char *stream_name,
		uint32_t partition_id)
{
	/* Bail out fast if offset 0 does not exist. */
	if (!log_record_exists(ns, stream_name, partition_id, 0)) {
		return -1;
	}

	/* Exponential search: find hi such that record[hi] does NOT exist. */
	int64_t lo = 0;
	int64_t hi = 1;

	while (log_record_exists(ns, stream_name, partition_id, hi)) {
		lo = hi;
		if (hi >= (int64_t)(INT64_MAX / 2)) {
			/* Overflow guard — extremely unlikely in practice. */
			hi = INT64_MAX;
			break;
		}
		hi *= 2;
	}

	/* Binary search: lo exists, hi does not — find the boundary. */
	while (hi - lo > 1) {
		int64_t mid = lo + (hi - lo) / 2;

		if (log_record_exists(ns, stream_name, partition_id, mid)) {
			lo = mid;
		}
		else {
			hi = mid;
		}
	}

	return lo;  /* maximum existing offset */
}

/*
 * Get or create the per-stream state struct.
 * On first creation, offset counters are reconstructed from storage so that
 * new produces after a restart do not overwrite existing log records.
 * Spawns one push loop thread per partition on first creation.
 */
static as_stream_state *
get_or_create_state(const char *stream_name, uint32_t num_partitions)
{
	char key[AS_STREAM_NAME_SZ];
	memset(key, 0, sizeof(key));
	memcpy(key, stream_name, strnlen(stream_name, AS_STREAM_NAME_MAX_LEN));

	/* Fast path: already exists. */
	as_stream_state *state = NULL;
	if (cf_shash_get(g_stream_states, key, &state) == CF_SHASH_OK) {
		return state;
	}

	/* Slow path: create under lock to prevent double-create races. */
	cf_mutex_lock(&g_stream_lock);

	if (cf_shash_get(g_stream_states, key, &state) == CF_SHASH_OK) {
		cf_mutex_unlock(&g_stream_lock);
		return state;
	}

	size_t state_sz = sizeof(as_stream_state)
			+ num_partitions * sizeof(as_stream_partition);
	state = (as_stream_state *)cf_malloc(state_sz);
	memset(state, 0, state_sz);
	memcpy(state->name, key, AS_STREAM_NAME_SZ);
	state->num_partitions = num_partitions;

	/*
	 * memset zeroed push_lock and push_cond to their correct initial values
	 * (both are 0-initialised per their cf_*_init macros). Initialise them
	 * explicitly for clarity.
	 */
	for (uint32_t i = 0; i < num_partitions; i++) {
		cf_mutex_init(&state->partitions[i].push_lock);
		cf_condition_init(&state->partitions[i].push_cond);
		state->partitions[i].sessions = NULL;
	}

	/*
	 * Reconstruct offset counters from storage before inserting into the
	 * shash.  The state is not yet visible to other threads here (still
	 * holding g_stream_lock, not yet in shash), so direct writes to
	 * offset_seq are safe without atomic operations.
	 *
	 * This prevents new produces from overwriting records written in a
	 * previous server run.
	 */
	as_namespace *recon_ns = as_namespace_get_byname(AS_STREAM_NS);

	if (recon_ns != NULL) {
		uint32_t reconstructed = 0;

		for (uint32_t i = 0; i < num_partitions; i++) {
			int64_t max_off = find_max_offset(recon_ns, key, i);

			if (max_off >= 0) {
				state->partitions[i].offset_seq = (uint64_t)(max_off + 1);
				reconstructed++;
				cf_info(AS_SERVICE,
						"as_stream_log: reconstructed stream=%.63s "
						"partition %u max_offset %ld → offset_seq %lu",
						key, i, max_off,
						state->partitions[i].offset_seq);
			}
		}

		if (reconstructed > 0) {
			cf_info(AS_SERVICE,
					"as_stream_log: offset reconstruction complete "
					"stream=%.63s %u/%u partitions had data",
					key, reconstructed, num_partitions);
		}
	}

	cf_shash_put(g_stream_states, key, &state);
	cf_mutex_unlock(&g_stream_lock);

	/* Spawn push loop thread for each partition. */
	for (uint32_t i = 0; i < num_partitions; i++) {
		push_thread_args *args = (push_thread_args *)cf_malloc(
				sizeof(push_thread_args));
		args->state        = state;
		args->partition_id = i;
		state->partitions[i].push_tid = cf_thread_create_detached(
				push_loop_thread, args);
	}

	cf_info(AS_SERVICE,
			"as_stream_log: new stream state stream=%.63s n_parts %u "
			"push threads spawned",
			stream_name, num_partitions);

	return state;
}

/*
 * IOPS done_cb — called on a transaction thread when the storage write
 * completes.  Sends the PROD_ACK (for ack_mode > 0) and rearms fd.
 */
static void
produce_done_cb(void *udata, int result)
{
	as_stream_produce_ctx *ctx = (as_stream_produce_ctx *)udata;

	cf_debug(AS_SERVICE,
			"as_stream_log: produce_done_cb result %d stream fd %d "
			"offset %ld partition %u ack_mode %u",
			result, CSFD(&ctx->fd_h->sock),
			ctx->offset, ctx->partition_id, ctx->ack_mode);

	if (ctx->ack_mode > 0) {
		if (result == AS_OK) {
			/* Send PROD_ACK with assigned offset, partition, timestamp. */
			struct __attribute__((packed)) {
				as_proto               hdr;
				as_stream_prod_ack_msg ack;
			} resp;

			resp.hdr.version = PROTO_VERSION;
			resp.hdr.type    = AS_PROTO_TYPE_STREAM_PROD_ACK;
			resp.hdr.sz      = sizeof(as_stream_prod_ack_msg);
			as_proto_swap(&resp.hdr);

			/*
			 * Body fields sent in big-endian (phase-3: simple cast swap).
			 * TODO: use cf_swap_to_be* helpers for each field.
			 */
			resp.ack.correlation_id = ctx->correlation_id;  /* already BE */
			resp.ack.offset         = (int64_t)cf_swap_to_be64(
					(uint64_t)ctx->offset);
			resp.ack.partition_id   = cf_swap_to_be32(ctx->partition_id);
			resp.ack.timestamp_ns   = cf_swap_to_be64(ctx->timestamp_ns);
			resp.ack.status         = AEROSTREAM_OK;

			cf_debug(AS_SERVICE,
					"as_stream_log: PROD_ACK fd %d client %s "
					"offset %ld partition %u ts %lu",
					CSFD(&ctx->fd_h->sock), ctx->fd_h->client,
					ctx->offset, ctx->partition_id, ctx->timestamp_ns);

			if (cf_socket_send_all(&ctx->fd_h->sock, &resp, sizeof(resp),
					MSG_NOSIGNAL, CF_SOCKET_TIMEOUT) < 0) {
				cf_warning(AS_SERVICE,
						"as_stream_log: PROD_ACK send failed fd %d errno %d",
						CSFD(&ctx->fd_h->sock), errno);
				as_end_of_transaction_force_close(ctx->fd_h);
				goto cleanup;
			}
		}
		else {
			cf_warning(AS_SERVICE,
					"as_stream_log: storage write failed result %d "
					"fd %d client %s stream fd %d offset %ld",
					result, CSFD(&ctx->fd_h->sock), ctx->fd_h->client,
					CSFD(&ctx->fd_h->sock), ctx->offset);

			/* Reuse send_err pattern inline since it's in aerostream.c scope. */
			struct __attribute__((packed)) {
				as_proto               hdr;
				as_stream_prod_ack_msg ack;
			} err_resp;

			err_resp.hdr.version = PROTO_VERSION;
			err_resp.hdr.type    = AS_PROTO_TYPE_STREAM_PROD_ACK;
			err_resp.hdr.sz      = sizeof(as_stream_prod_ack_msg);
			as_proto_swap(&err_resp.hdr);

			err_resp.ack.correlation_id = ctx->correlation_id;
			err_resp.ack.offset         = -1;
			err_resp.ack.partition_id   = 0;
			err_resp.ack.timestamp_ns   = 0;
			err_resp.ack.status         = AEROSTREAM_ERR_STORAGE;

			if (cf_socket_send_all(&ctx->fd_h->sock, &err_resp,
					sizeof(err_resp), MSG_NOSIGNAL,
					CF_SOCKET_TIMEOUT) < 0) {
				as_end_of_transaction_force_close(ctx->fd_h);
				goto cleanup;
			}
		}

		as_end_of_transaction_ok(ctx->fd_h);
	}

cleanup:
	iops_origin_destroy(&ctx->iops_orig);
	cf_free(ctx);
}

/*
 * Public API.
 */

void
as_stream_log_module_init(void)
{
	cf_debug(AS_SERVICE, "as_stream_log: init");
	as_stream_groups_module_init();
	as_stream_replay_module_init();
	as_stream_pubsub_module_init();

	cf_mutex_init(&g_stream_lock);

	g_stream_states = cf_shash_create(cf_shash_fn_zstr,
			AS_STREAM_NAME_SZ,
			sizeof(as_stream_state *),
			64,
			true);

	if (g_stream_states == NULL) {
		cf_crash(AS_SERVICE, "as_stream_log: failed to create stream state hash");
	}

	/* Warn if namespace not configured — writes will fail at runtime. */
	if (as_namespace_get_byname(AS_STREAM_NS) == NULL) {
		cf_warning(AS_SERVICE,
				"as_stream_log: namespace '%s' not found in server config — "
				"produce writes will fail. Add it to aerospike.conf.",
				AS_STREAM_NS);
	}
	else {
		cf_info(AS_SERVICE,
				"as_stream_log: ready, namespace '%s' found", AS_STREAM_NS);
	}
}

bool
as_stream_log_handle_produce(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"as_stream_log: handle_produce enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, (uint64_t)proto->sz);

	/*
	 * Declare msg before any goto so we never jump over its initialization.
	 * Set to NULL when the proto body is too short; the inline_err label
	 * checks proto->sz before dereferencing it.
	 */
	as_stream_produce_msg *msg = (proto->sz >= sizeof(as_stream_produce_msg))
			? (as_stream_produce_msg *)proto->body
			: NULL;

	if (msg == NULL) {
		cf_warning(AS_SERVICE,
				"as_stream_log: PRODUCE too short got %lu need %zu "
				"fd %d client %s",
				(uint64_t)proto->sz, sizeof(as_stream_produce_msg),
				CSFD(&fd_h->sock), fd_h->client);
		goto inline_err;
	}

	/* Byte-swap body fields from network (big-endian) to host order. */
	uint64_t corr_id      = cf_swap_from_be64(msg->hdr.correlation_id);
	uint16_t hdr_count    = cf_swap_from_be16(msg->rec_hdr.headers_count);
	uint32_t payload_sz   = cf_swap_from_be32(msg->rec_hdr.payload_size);
	uint8_t  ack_mode     = msg->ack_mode;

	cf_debug(AS_SERVICE,
			"as_stream_log: PRODUCE stream=%.63s corr_id %lu "
			"partition_key=%.63s ack_mode %u hdr_count %u payload_sz %u "
			"fd %d client %s",
			msg->hdr.stream_name, corr_id,
			msg->partition_key, ack_mode, hdr_count, payload_sz,
			CSFD(&fd_h->sock), fd_h->client);

	/* Validate that proto body covers fixed struct + payload. */
	size_t min_body_sz = sizeof(as_stream_produce_msg) + payload_sz;
	if (proto->sz < min_body_sz) {
		cf_warning(AS_SERVICE,
				"as_stream_log: PRODUCE body too small for payload "
				"got %lu need %zu fd %d",
				(uint64_t)proto->sz, min_body_sz, CSFD(&fd_h->sock));
		goto inline_err;
	}

	/* Phase-3: silently skip header entries (TODO phase-5 hdrs bin). */
	if (hdr_count > 0) {
		cf_debug(AS_SERVICE,
				"as_stream_log: PRODUCE stream=%.63s hdr_count %u ignored "
				"(phase-3 limitation)",
				msg->hdr.stream_name, hdr_count);
	}

	/* Payload bytes start immediately after the fixed struct. */
	const uint8_t *payload = proto->body + sizeof(as_stream_produce_msg);

	/* Get config (creates default config on first produce to this stream). */
	as_stream_config cfg;
	as_stream_config_get_or_default((const char *)msg->hdr.stream_name, &cfg);

	/* Get or create stream state. */
	as_stream_state *state = get_or_create_state(
			(const char *)msg->hdr.stream_name, cfg.num_partitions);

	/* Route to partition via FNV-32a of partition_key. */
	uint32_t partition_id = fnv32a(msg->partition_key,
			(uint32_t)strnlen((char *)msg->partition_key,
					sizeof(msg->partition_key)))
			% state->num_partitions;

	/* Atomically assign the next offset (returns old value = assigned offset). */
	int64_t offset = (int64_t)as_faa_uint64(
			&state->partitions[partition_id].offset_seq, 1);

	uint64_t timestamp_ns = wall_clock_ns();

	cf_debug(AS_SERVICE,
			"as_stream_log: PRODUCE stream=%.63s partition %u offset %ld "
			"ts_ns %lu payload_sz %u",
			msg->hdr.stream_name, partition_id, offset,
			timestamp_ns, payload_sz);

	/* Build the log record key: "{stream}:{partition}:{offset}" */
	char log_key[LOG_KEY_MAX_LEN];
	int  log_key_len = snprintf(log_key, sizeof(log_key), "%.63s:%u:%ld",
			msg->hdr.stream_name, partition_id, offset);

	if (log_key_len < 0 || (size_t)log_key_len >= sizeof(log_key)) {
		cf_warning(AS_SERVICE,
				"as_stream_log: log key too long, stream=%.63s "
				"fd %d client %s",
				msg->hdr.stream_name, CSFD(&fd_h->sock), fd_h->client);
		goto inline_err;
	}

	cf_debug(AS_SERVICE,
			"as_stream_log: log key '%s' digest computing", log_key);

	/* Compute the record key digest from the key string. */
	cf_digest keyd;
	cf_digest_compute(log_key, (size_t)log_key_len, &keyd);

	/*
	 * Fan-out to pub/sub subscribers before submitting the storage write.
	 * The payload pointer (proto->body + offset) is valid here since proto
	 * is not freed until as_stream_dispatch returns after handle_produce.
	 * Ephemeral delivery — no durability, no offset tracking.
	 */
	as_stream_pubsub_fanout(
			(const char *)msg->hdr.stream_name,
			partition_id, offset, (int64_t)timestamp_ns,
			payload, payload_sz);

	/* Build the cl_msg for the durable storage write. */
	cl_msg *write_msgp = build_log_write_msg(
			cfg.ttl_seconds, payload, payload_sz, (int64_t)timestamp_ns, offset);

	/* Allocate context — lives until produce_done_cb completes. */
	as_stream_produce_ctx *ctx = (as_stream_produce_ctx *)cf_malloc(
			sizeof(as_stream_produce_ctx));
	ctx->fd_h           = fd_h;
	ctx->correlation_id = msg->hdr.correlation_id;  /* keep in wire BE order */
	ctx->offset         = offset;
	ctx->partition_id   = partition_id;
	ctx->timestamp_ns   = timestamp_ns;
	ctx->ack_mode       = ack_mode;

	ctx->iops_orig.msgp       = write_msgp;
	ctx->iops_orig.filter_exp = NULL;
	ctx->iops_orig.expops     = NULL;
	ctx->iops_orig.check_cb   = NULL;
	ctx->iops_orig.done_cb    = produce_done_cb;
	ctx->iops_orig.udata      = ctx;

	/* Set up and enqueue the internal write transaction. */
	as_transaction tr;
	as_transaction_init_iops(&tr, NULL, &keyd, &ctx->iops_orig);
	as_transaction_set_msg_field_flag(&tr, AS_MSG_FIELD_TYPE_SET);

	cf_debug(AS_SERVICE,
			"as_stream_log: enqueuing IOPS write stream=%.63s "
			"key='%s' ack_mode %u",
			msg->hdr.stream_name, log_key, ack_mode);

	as_service_enqueue_internal(&tr);

	if (ack_mode == 0) {
		/* Fire-and-forget: rearm fd now, done_cb just cleans up. */
		cf_debug(AS_SERVICE,
				"as_stream_log: ack_mode=0 rearming fd %d immediately",
				CSFD(&fd_h->sock));
		return true;
	}

	/* ack_mode > 0: done_cb sends PROD_ACK and rearms fd. */
	cf_debug(AS_SERVICE,
			"as_stream_log: ack_mode=%u waiting for done_cb fd %d",
			ack_mode, CSFD(&fd_h->sock));
	return false;

inline_err:
	/* No ctx was allocated; send error inline and rearm. */
	{
		struct __attribute__((packed)) {
			as_proto               hdr;
			as_stream_prod_ack_msg ack;
		} err;

		err.hdr.version = PROTO_VERSION;
		err.hdr.type    = AS_PROTO_TYPE_STREAM_PROD_ACK;
		err.hdr.sz      = sizeof(as_stream_prod_ack_msg);
		as_proto_swap(&err.hdr);

		err.ack.correlation_id = (proto->sz >= sizeof(as_stream_produce_msg))
				? msg->hdr.correlation_id : 0;
		err.ack.offset         = -1;
		err.ack.partition_id   = 0;
		err.ack.timestamp_ns   = 0;
		err.ack.status         = AEROSTREAM_ERR_NOT_FOUND;

		if (cf_socket_send_all(&fd_h->sock, &err, sizeof(err),
				MSG_NOSIGNAL, CF_SOCKET_TIMEOUT) < 0) {
			cf_warning(AS_SERVICE,
					"as_stream_log: inline_err send failed fd %d errno %d",
					CSFD(&fd_h->sock), errno);
			as_end_of_transaction_force_close(fd_h);
			return false;  /* already closed */
		}
	}
	return true;
}

/*
 * Look up an existing stream state without creating one.
 * Returns NULL if the stream has no state yet (not yet produced to).
 */
static as_stream_state *
get_state(const char *stream_name)
{
	char key[AS_STREAM_NAME_SZ];
	memset(key, 0, sizeof(key));
	memcpy(key, stream_name, strnlen(stream_name, AS_STREAM_NAME_MAX_LEN));

	as_stream_state *state = NULL;
	cf_shash_get(g_stream_states, key, &state);
	return state;
}

/*
 * Register a single-partition push session.
 * Allocates a session, appends to the partition's sessions list, and
 * signals the push loop to start delivery.
 * Called under NO lock — acquires push_lock internally.
 */
static void
register_session(as_stream_state *state, uint32_t partition_id,
		as_file_handle *fd_h, as_stream_fd_lock *send_lock,
		uint64_t correlation_id, const char *group_name,
		int64_t start_offset, uint32_t max_in_flight)
{
	as_stream_session *sess = (as_stream_session *)cf_malloc(
			sizeof(as_stream_session));

	sess->fd_h           = fd_h;
	sess->send_lock      = send_lock;
	sess->correlation_id = correlation_id;
	memcpy(sess->group,  group_name,  AS_STREAM_NAME_SZ);
	memcpy(sess->stream, state->name, AS_STREAM_NAME_SZ);
	sess->partition_id   = partition_id;
	sess->next_offset    = start_offset;
	sess->max_in_flight  = (max_in_flight == 0) ? 10 : max_in_flight;
	sess->in_flight      = 0;
	sess->active         = true;

	as_stream_partition *part = &state->partitions[partition_id];

	cf_mutex_lock(&part->push_lock);

	sess->next       = part->sessions;
	part->sessions   = sess;

	send_lock->ref_count++;

	cf_condition_signal(&part->push_cond);
	cf_mutex_unlock(&part->push_lock);

	cf_debug(AS_SERVICE,
			"as_stream_log: registered session stream=%.63s partition %u "
			"group=%.63s start_offset %ld max_in_flight %u fd %d",
			state->name, partition_id, group_name,
			start_offset, sess->max_in_flight, CSFD(&fd_h->sock));
}

bool
as_stream_log_handle_consume(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"as_stream_log: handle_consume enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, (uint64_t)proto->sz);

	if (proto->sz < sizeof(as_stream_consume_msg)) {
		cf_warning(AS_SERVICE,
				"as_stream_log: CONSUME too short got %lu need %zu fd %d",
				(uint64_t)proto->sz, sizeof(as_stream_consume_msg), CSFD(&fd_h->sock));
		return true;  /* rearm — nothing registered */
	}

	as_stream_consume_msg *msg = (as_stream_consume_msg *)proto->body;

	uint64_t corr_id      = cf_swap_from_be64(msg->hdr.correlation_id);
	uint32_t partition_id = cf_swap_from_be32(msg->partition_id);
	uint8_t  seek_type    = msg->seek_type;
	int64_t  seek_val     = (int64_t)cf_swap_from_be64((uint64_t)msg->seek_value);
	uint32_t max_flight   = cf_swap_from_be32(msg->max_in_flight);

	if (partition_id == 0xFFFFFFFF) {
		cf_debug(AS_SERVICE,
				"as_stream_log: CONSUME stream=%.63s group=%.63s corr_id %lu "
				"partition=ALL seek_type %u seek_val %ld max_in_flight %u "
				"fd %d client %s",
				msg->hdr.stream_name, msg->group_name, corr_id,
				seek_type, seek_val, max_flight,
				CSFD(&fd_h->sock), fd_h->client);
	}
	else {
		cf_debug(AS_SERVICE,
				"as_stream_log: CONSUME stream=%.63s group=%.63s corr_id %lu "
				"partition %u seek_type %u seek_val %ld max_in_flight %u "
				"fd %d client %s",
				msg->hdr.stream_name, msg->group_name, corr_id,
				partition_id, seek_type, seek_val, max_flight,
				CSFD(&fd_h->sock), fd_h->client);
	}

	/* Get config; if no stream exists yet, use defaults. */
	as_stream_config cfg;
	as_stream_config_get_or_default((const char *)msg->hdr.stream_name, &cfg);

	/* Get or create stream state (and push threads if needed). */
	as_stream_state *state = get_or_create_state(
			(const char *)msg->hdr.stream_name, cfg.num_partitions);

	/* Allocate one shared send_lock for this fd_h (covers all partitions). */
	as_stream_fd_lock *send_lock = (as_stream_fd_lock *)cf_malloc(
			sizeof(as_stream_fd_lock));
	cf_mutex_init(&send_lock->mtx);
	send_lock->ref_count = 0;  /* register_session increments */

	uint32_t first_part = 0;
	uint32_t last_part  = cfg.num_partitions - 1;

	if (partition_id != 0xFFFFFFFF) {
		if (partition_id >= cfg.num_partitions) {
			cf_warning(AS_SERVICE,
					"as_stream_log: CONSUME partition %u out of range "
					"(stream has %u partitions) fd %d",
					partition_id, cfg.num_partitions, CSFD(&fd_h->sock));
			cf_free(send_lock);
			return true;
		}
		first_part = last_part = partition_id;
	}

	for (uint32_t p = first_part; p <= last_part; p++) {
		/* Determine start offset for this partition. */
		int64_t start_offset;

		switch (seek_type) {
		case AS_STREAM_SEEK_LATEST:
			/* Start from the next record to be produced. */
			start_offset = (int64_t)as_load_uint64(
					&state->partitions[p].offset_seq);
			break;
		case AS_STREAM_SEEK_OFFSET:
			start_offset = seek_val;
			break;
		case AS_STREAM_SEEK_TIMESTAMP:
			/* TODO phase-5: binary search by timestamp. Fall through. */
			cf_debug(AS_SERVICE,
					"as_stream_log: CONSUME seek_type TIMESTAMP not "
					"implemented, falling back to EARLIEST");
			/* fall through */
		case AS_STREAM_SEEK_EARLIEST:
		default:
			start_offset = 0;
			break;
		}

		cf_debug(AS_SERVICE,
				"as_stream_log: CONSUME registering stream=%.63s "
				"partition %u start_offset %ld",
				state->name, p, start_offset);

		register_session(state, p, fd_h, send_lock,
				corr_id,   /* host order — send_stream_record converts to BE */
				(const char *)msg->group_name,
				start_offset, max_flight);
	}

	/*
	 * If no sessions were registered (partition out of range), send_lock
	 * ref_count is still 0 — free it.
	 */
	if (send_lock->ref_count == 0) {
		cf_free(send_lock);
	}

	cf_info(AS_SERVICE,
			"as_stream_log: CONSUME registered stream=%.63s group=%.63s "
			"partitions %u-%u fd %d client %s",
			msg->hdr.stream_name, msg->group_name,
			first_part, last_part,
			CSFD(&fd_h->sock), fd_h->client);

	/* Rearm so ACKs and SEEK/UNSUB can be received on this fd. */
	return true;
}

bool
as_stream_log_handle_ack(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"as_stream_log: handle_ack enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, (uint64_t)proto->sz);

	if (proto->sz < sizeof(as_stream_ack_msg)) {
		cf_warning(AS_SERVICE,
				"as_stream_log: ACK too short got %lu need %zu fd %d",
				(uint64_t)proto->sz, sizeof(as_stream_ack_msg), CSFD(&fd_h->sock));
		return true;
	}

	as_stream_ack_msg *msg = (as_stream_ack_msg *)proto->body;

	uint32_t partition_id = cf_swap_from_be32(msg->partition_id);
	int64_t  offset       = (int64_t)cf_swap_from_be64((uint64_t)msg->offset);

	cf_debug(AS_SERVICE,
			"as_stream_log: ACK stream=%.63s group=%.63s partition %u "
			"offset %ld fd %d client %s",
			msg->hdr.stream_name, msg->group_name,
			partition_id, offset,
			CSFD(&fd_h->sock), fd_h->client);

	/* Locate the stream state. */
	as_stream_state *state = get_state((const char *)msg->hdr.stream_name);

	if (state == NULL) {
		cf_warning(AS_SERVICE,
				"as_stream_log: ACK for unknown stream=%.63s fd %d",
				msg->hdr.stream_name, CSFD(&fd_h->sock));
		return true;
	}

	if (partition_id >= state->num_partitions) {
		cf_warning(AS_SERVICE,
				"as_stream_log: ACK partition %u out of range "
				"(stream has %u partitions) fd %d",
				partition_id, state->num_partitions, CSFD(&fd_h->sock));
		return true;
	}

	as_stream_partition *part = &state->partitions[partition_id];

	/* Find the session for this (group, fd_h) pair and decrement in_flight. */
	cf_mutex_lock(&part->push_lock);

	as_stream_session *sess = part->sessions;
	bool found = false;

	while (sess != NULL) {
		if (sess->active && sess->fd_h == fd_h
				&& strncmp(sess->group, (char *)msg->group_name,
						AS_STREAM_NAME_MAX_LEN) == 0) {
			if (sess->in_flight > 0) {
				sess->in_flight--;
			}
			found = true;
			break;
		}
		sess = sess->next;
	}

	cf_condition_signal(&part->push_cond);
	cf_mutex_unlock(&part->push_lock);

	if (!found) {
		cf_debug(AS_SERVICE,
				"as_stream_log: ACK for stream=%.63s group=%.63s partition %u "
				"no matching session (already cleaned up?)",
				msg->hdr.stream_name, msg->group_name, partition_id);
		return true;
	}

	/* Persist committed offset asynchronously (fire-and-forget). */
	int64_t head = (int64_t)as_load_uint64(&part->offset_seq) - 1;
	int64_t lag  = (head >= offset) ? (head - offset) : 0;

	as_stream_groups_commit(
			(const char *)msg->hdr.stream_name,
			(const char *)msg->group_name,
			partition_id, offset, lag);

	cf_debug(AS_SERVICE,
			"as_stream_log: ACK committed stream=%.63s group=%.63s "
			"partition %u offset %ld lag %ld fd %d",
			msg->hdr.stream_name, msg->group_name,
			partition_id, offset, lag,
			CSFD(&fd_h->sock));

	return true;
}

bool
as_stream_log_handle_seek(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"as_stream_log: handle_seek enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, (uint64_t)proto->sz);

	if (proto->sz < sizeof(as_stream_seek_msg)) {
		cf_warning(AS_SERVICE,
				"as_stream_log: SEEK too short got %lu need %zu fd %d",
				(uint64_t)proto->sz, sizeof(as_stream_seek_msg), CSFD(&fd_h->sock));
		return true;
	}

	as_stream_seek_msg *msg = (as_stream_seek_msg *)proto->body;

	uint32_t partition_id = cf_swap_from_be32(msg->partition_id);
	uint8_t  seek_type    = msg->seek_type;
	int64_t  seek_val     = (int64_t)cf_swap_from_be64((uint64_t)msg->seek_value);

	cf_debug(AS_SERVICE,
			"as_stream_log: SEEK stream=%.63s group=%.63s partition %u "
			"seek_type %u seek_val %ld fd %d client %s",
			msg->hdr.stream_name, msg->group_name,
			partition_id, seek_type, seek_val,
			CSFD(&fd_h->sock), fd_h->client);

	as_stream_state *state = get_state((const char *)msg->hdr.stream_name);

	if (state == NULL) {
		cf_warning(AS_SERVICE,
				"as_stream_log: SEEK for unknown stream=%.63s fd %d",
				msg->hdr.stream_name, CSFD(&fd_h->sock));
		return true;
	}

	if (partition_id >= state->num_partitions) {
		cf_warning(AS_SERVICE,
				"as_stream_log: SEEK partition %u out of range "
				"(stream has %u partitions) fd %d",
				partition_id, state->num_partitions, CSFD(&fd_h->sock));
		return true;
	}

	as_stream_partition *part = &state->partitions[partition_id];

	/* head_offset = last assigned offset; -1 if no records yet. */
	int64_t head_offset = (int64_t)as_load_uint64(&part->offset_seq) - 1;

	as_namespace *ns = as_namespace_get_byname(AS_STREAM_NS);

	int64_t new_offset = as_stream_replay_resolve(ns,
			(const char *)msg->hdr.stream_name, partition_id,
			seek_type, seek_val, head_offset);

	cf_debug(AS_SERVICE,
			"as_stream_log: SEEK resolved stream=%.63s partition %u "
			"seek_type %u seek_val %ld → new_offset %ld",
			msg->hdr.stream_name, partition_id,
			seek_type, seek_val, new_offset);

	/* Update the session under push_lock. */
	cf_mutex_lock(&part->push_lock);

	as_stream_session *sess = part->sessions;
	bool found = false;

	while (sess != NULL) {
		if (sess->active && sess->fd_h == fd_h
				&& strncmp(sess->group, (char *)msg->group_name,
						AS_STREAM_NAME_MAX_LEN) == 0) {
			sess->next_offset = new_offset;
			sess->in_flight   = 0;   /* abandon in-flight records at old pos */
			found = true;

			cf_debug(AS_SERVICE,
					"as_stream_log: SEEK updated session stream=%.63s "
					"group=%.63s partition %u new_offset %ld",
					msg->hdr.stream_name, msg->group_name,
					partition_id, new_offset);
			break;
		}
		sess = sess->next;
	}

	cf_condition_signal(&part->push_cond);
	cf_mutex_unlock(&part->push_lock);

	if (!found) {
		cf_debug(AS_SERVICE,
				"as_stream_log: SEEK stream=%.63s group=%.63s partition %u "
				"no matching session",
				msg->hdr.stream_name, msg->group_name, partition_id);
		return true;
	}

	/* Reset the committed offset in storage to match the new position. */
	int64_t lag = (head_offset >= new_offset) ? (head_offset - new_offset) : 0;

	as_stream_groups_commit(
			(const char *)msg->hdr.stream_name,
			(const char *)msg->group_name,
			partition_id, new_offset, lag);

	cf_info(AS_SERVICE,
			"as_stream_log: SEEK complete stream=%.63s group=%.63s "
			"partition %u new_offset %ld lag %ld fd %d client %s",
			msg->hdr.stream_name, msg->group_name,
			partition_id, new_offset, lag,
			CSFD(&fd_h->sock), fd_h->client);

	return true;
}

bool
as_stream_log_handle_unsub(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"as_stream_log: handle_unsub enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, (uint64_t)proto->sz);

	if (proto->sz < sizeof(as_stream_unsub_msg)) {
		cf_warning(AS_SERVICE,
				"as_stream_log: UNSUB too short got %lu need %zu fd %d",
				(uint64_t)proto->sz, sizeof(as_stream_unsub_msg), CSFD(&fd_h->sock));
		return true;
	}

	as_stream_unsub_msg *msg = (as_stream_unsub_msg *)proto->body;

	cf_debug(AS_SERVICE,
			"as_stream_log: UNSUB stream=%.63s fd %d client %s",
			msg->hdr.stream_name, CSFD(&fd_h->sock), fd_h->client);

	as_stream_state *state = get_state((const char *)msg->hdr.stream_name);

	if (state == NULL) {
		cf_debug(AS_SERVICE,
				"as_stream_log: UNSUB for unknown stream=%.63s fd %d",
				msg->hdr.stream_name, CSFD(&fd_h->sock));
		return true;
	}

	uint32_t killed = 0;

	/* Mark all sessions for this fd_h as inactive across all partitions. */
	for (uint32_t p = 0; p < state->num_partitions; p++) {
		as_stream_partition *part = &state->partitions[p];

		cf_mutex_lock(&part->push_lock);

		uint32_t killed_here  = 0;
		as_stream_session *sess = part->sessions;

		while (sess != NULL) {
			if (sess->active && sess->fd_h == fd_h) {
				sess->active = false;
				killed_here++;
				cf_debug(AS_SERVICE,
						"as_stream_log: UNSUB killed session "
						"stream=%.63s partition %u group=%.63s",
						state->name, p, sess->group);
			}
			sess = sess->next;
		}

		if (killed_here > 0) {
			cf_condition_signal(&part->push_cond);
		}

		killed += killed_here;
		cf_mutex_unlock(&part->push_lock);
	}

	cf_info(AS_SERVICE,
			"as_stream_log: UNSUB stream=%.63s fd %d client %s "
			"killed %u sessions",
			msg->hdr.stream_name, CSFD(&fd_h->sock),
			fd_h->client, killed);

	return true;
}
