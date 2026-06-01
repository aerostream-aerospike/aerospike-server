/*
 * as_stream_pubsub.h
 *
 * Ephemeral pub/sub: in-memory subscription registry with real-time fan-out.
 * No offset tracking, no durability — records are lost if the subscriber is
 * disconnected.
 *
 * Registry: cf_shash keyed by topic name (char[64]).
 * Each topic holds a cf_vector of (fd_h, correlation_id) subscriber entries.
 * A single global cf_mutex serialises all registry operations.
 *
 * Fan-out is called synchronously from the produce path (before the storage
 * write is submitted) so the payload pointer in proto->body is still valid.
 * Dead subscribers (failed socket write) are removed from the vector at the
 * end of the fan-out pass.
 */

#pragma once

#include <stdint.h>

#include "base/transaction.h"

#define AS_STREAM_PUBSUB_TOPIC_SZ  64   /* matches as_stream_sub_msg.topic */

/*
 * Called from as_stream_log_module_init().
 */
void as_stream_pubsub_module_init(void);

/*
 * Register an ephemeral subscriber on a topic.
 * correlation_id is in host byte order — it will be sent in big-endian in
 * each STREAM_RECORD delivery.
 */
void as_stream_pubsub_subscribe(as_file_handle *fd_h,
		uint64_t correlation_id, const char *topic);

/*
 * Remove one subscriber from a specific topic.
 * Called when the client sends STREAM_UNSUB (unsub_type == 0x01).
 */
void as_stream_pubsub_unsubscribe(as_file_handle *fd_h, const char *topic);

/*
 * Fan out a newly produced record to all subscribers on the matching topic.
 * stream_name is used as the topic key.  Called from the produce path.
 * Dead connections are pruned inline.
 */
void as_stream_pubsub_fanout(const char *stream_name,
		uint32_t partition_id, int64_t offset, int64_t ts_ns,
		const uint8_t *payload, uint32_t payload_sz);

/*
 * Remove fd_h from every topic it is subscribed to.
 * Called when a client connection closes without sending STREAM_UNSUB,
 * preventing dead subscriber entries from accumulating in the registry.
 */
void as_stream_pubsub_deregister_conn(as_file_handle *fd_h);
