/*
 * as_stream_pubsub.c
 *
 * In-memory pub/sub registry and fan-out.
 */

#include "modules/aerostream/as_stream_pubsub.h"
#include "modules/aerostream/as_stream_log.h"

#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "cf_mutex.h"
#include "log.h"
#include "shash.h"
#include "vector.h"

#include "base/transaction.h"

/*
 * One entry in the subscriber vector per topic.
 */
typedef struct {
	as_file_handle *fd_h;
	uint64_t        correlation_id;  /* host order; converted to BE on send */
} as_pubsub_entry;

/*
 * Module globals.
 *
 * g_pubsub_registry: key = char[AS_STREAM_PUBSUB_TOPIC_SZ], value = cf_vector*
 * g_pubsub_lock:     serialises all registry operations.
 */
static cf_shash *g_pubsub_registry;
static cf_mutex  g_pubsub_lock;

/*
 * Build a fixed-size, zero-padded topic key.
 */
static void
make_topic_key(char key[AS_STREAM_PUBSUB_TOPIC_SZ], const char *topic)
{
	memset(key, 0, AS_STREAM_PUBSUB_TOPIC_SZ);
	memcpy(key, topic, strnlen(topic, AS_STREAM_PUBSUB_TOPIC_SZ - 1));
}

/*
 * Public API.
 */

void
as_stream_pubsub_module_init(void)
{
	cf_mutex_init(&g_pubsub_lock);

	g_pubsub_registry = cf_shash_create(cf_shash_fn_zstr,
			AS_STREAM_PUBSUB_TOPIC_SZ,
			sizeof(cf_vector *),
			32,    /* initial topic buckets */
			false); /* g_pubsub_lock already serialises all operations */

	if (g_pubsub_registry == NULL) {
		cf_crash(AS_SERVICE, "as_stream_pubsub: failed to create registry");
	}

	cf_info(AS_SERVICE, "as_stream_pubsub: registry ready");
}

void
as_stream_pubsub_subscribe(as_file_handle *fd_h,
		uint64_t correlation_id, const char *topic)
{
	char key[AS_STREAM_PUBSUB_TOPIC_SZ];
	make_topic_key(key, topic);

	cf_debug(AS_SERVICE,
			"as_stream_pubsub: subscribe topic=%.63s corr_id %lu fd %d",
			topic, correlation_id, CSFD(&fd_h->sock));

	cf_mutex_lock(&g_pubsub_lock);

	cf_vector *v = NULL;
	if (cf_shash_get(g_pubsub_registry, key, &v) != CF_SHASH_OK || v == NULL) {
		/* First subscriber on this topic — create the vector. */
		v = cf_vector_create(sizeof(as_pubsub_entry), 4, VECTOR_FLAG_INITZERO);
		cf_shash_put(g_pubsub_registry, key, &v);

		cf_debug(AS_SERVICE,
				"as_stream_pubsub: new topic=%.63s", topic);
	}

	as_pubsub_entry entry = { .fd_h = fd_h, .correlation_id = correlation_id };
	cf_vector_append(v, &entry);

	cf_debug(AS_SERVICE,
			"as_stream_pubsub: subscribe ok topic=%.63s subscribers %u",
			topic, cf_vector_size(v));

	cf_mutex_unlock(&g_pubsub_lock);
}

void
as_stream_pubsub_unsubscribe(as_file_handle *fd_h, const char *topic)
{
	char key[AS_STREAM_PUBSUB_TOPIC_SZ];
	make_topic_key(key, topic);

	cf_debug(AS_SERVICE,
			"as_stream_pubsub: unsubscribe topic=%.63s fd %d",
			topic, CSFD(&fd_h->sock));

	cf_mutex_lock(&g_pubsub_lock);

	cf_vector *v = NULL;
	if (cf_shash_get(g_pubsub_registry, key, &v) != CF_SHASH_OK || v == NULL) {
		cf_mutex_unlock(&g_pubsub_lock);
		return;
	}

	uint32_t n = cf_vector_size(v);

	for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
		as_pubsub_entry entry;
		cf_vector_get(v, (uint32_t)i, &entry);

		if (entry.fd_h == fd_h) {
			cf_vector_delete(v, (uint32_t)i);
			cf_debug(AS_SERVICE,
					"as_stream_pubsub: unsubscribed topic=%.63s "
					"fd %d remaining %u",
					topic, CSFD(&fd_h->sock), cf_vector_size(v));
			break;
		}
	}

	cf_mutex_unlock(&g_pubsub_lock);
}

void
as_stream_pubsub_fanout(const char *stream_name,
		uint32_t partition_id, int64_t offset, int64_t ts_ns,
		const uint8_t *payload, uint32_t payload_sz,
		const uint8_t *hdrs, uint32_t hdrs_sz)
{
	char key[AS_STREAM_PUBSUB_TOPIC_SZ];
	make_topic_key(key, stream_name);

	cf_mutex_lock(&g_pubsub_lock);

	cf_vector *v = NULL;
	if (cf_shash_get(g_pubsub_registry, key, &v) != CF_SHASH_OK
			|| v == NULL
			|| cf_vector_size(v) == 0) {
		cf_mutex_unlock(&g_pubsub_lock);
		return;
	}

	uint32_t n = cf_vector_size(v);

	cf_debug(AS_SERVICE,
			"as_stream_pubsub: fanout topic=%.63s partition %u offset %ld "
			"payload_sz %u subscribers %u",
			stream_name, partition_id, offset, payload_sz, n);

	/*
	 * Iterate the subscriber vector under the lock.  Send to each subscriber;
	 * if the write fails, null out the fd_h field so the compaction pass
	 * below can identify and remove dead entries.
	 *
	 * Phase-6 note: holding the lock during socket writes keeps the code
	 * simple.  The timeout is CF_SOCKET_TIMEOUT (10 s) per write, which is
	 * acceptable for an ephemeral pub/sub tier.  A lock-free snapshot
	 * approach can replace this in a later pass if needed.
	 */
	uint32_t delivered = 0;
	uint32_t dead      = 0;

	for (uint32_t i = 0; i < n; i++) {
		as_pubsub_entry entry;
		cf_vector_get(v, i, &entry);

		if (entry.fd_h == NULL) {
			continue;  /* already dead from a prior pass */
		}

		bool ok = as_stream_log_send_record(entry.fd_h,
				entry.correlation_id,
				partition_id, offset, ts_ns,
				payload, payload_sz, hdrs, hdrs_sz);

		if (ok) {
			delivered++;
		}
		else {
			cf_debug(AS_SERVICE,
					"as_stream_pubsub: fanout send failed fd %d, "
					"removing subscriber from topic=%.63s",
					CSFD(&entry.fd_h->sock), stream_name);

			/* Null out so the compaction pass removes it. */
			entry.fd_h = NULL;
			cf_vector_set(v, i, &entry);
			dead++;
		}
	}

	/* Compact: remove dead entries scanning backwards so indices stay valid. */
	if (dead > 0) {
		for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
			as_pubsub_entry entry;
			cf_vector_get(v, (uint32_t)i, &entry);

			if (entry.fd_h == NULL) {
				cf_vector_delete(v, (uint32_t)i);
			}
		}
	}

	cf_mutex_unlock(&g_pubsub_lock);

	cf_debug(AS_SERVICE,
			"as_stream_pubsub: fanout topic=%.63s delivered %u dead %u",
			stream_name, delivered, dead);
}

/*
 * cf_shash_reduce callback for deregister_conn.
 * For each topic vector, remove any entry whose fd_h matches the target.
 */
typedef struct {
	as_file_handle *target;
	uint32_t        removed;
} deregister_ctx;

static int
deregister_reduce_cb(const void *key, void *val, void *udata)
{
	(void)key;
	cf_vector       *v   = *(cf_vector **)val;
	deregister_ctx  *ctx = (deregister_ctx *)udata;

	if (v == NULL) {
		return CF_SHASH_OK;
	}

	uint32_t n = cf_vector_size(v);

	for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
		as_pubsub_entry entry;
		cf_vector_get(v, (uint32_t)i, &entry);

		if (entry.fd_h == ctx->target) {
			cf_vector_delete(v, (uint32_t)i);
			ctx->removed++;
		}
	}

	return CF_SHASH_OK;
}

void
as_stream_pubsub_deregister_conn(as_file_handle *fd_h)
{
	cf_debug(AS_SERVICE,
			"as_stream_pubsub: deregister_conn fd %d", CSFD(&fd_h->sock));

	deregister_ctx ctx = { .target = fd_h, .removed = 0 };

	cf_mutex_lock(&g_pubsub_lock);

	/*
	 * Scan every topic vector and remove all entries for this fd_h.
	 * O(topics × subscribers_per_topic) — acceptable; this is an exceptional
	 * path (connection closed without sending STREAM_UNSUB).
	 */
	cf_shash_reduce(g_pubsub_registry, deregister_reduce_cb, &ctx);

	cf_mutex_unlock(&g_pubsub_lock);

	if (ctx.removed > 0) {
		cf_info(AS_SERVICE,
				"as_stream_pubsub: deregister_conn removed %u stale "
				"subscriptions fd %d",
				ctx.removed, CSFD(&fd_h->sock));
	}
}
