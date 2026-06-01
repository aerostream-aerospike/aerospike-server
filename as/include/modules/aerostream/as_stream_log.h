/*
 * as_stream_log.h
 *
 * Produce path: offset assignment, storage write, PROD_ACK.
 *
 * In-memory state: per-stream partition structs with cf_atomic offset counters.
 *
 * Storage write: IOPS transaction writing bins payload/ts/offset to
 * namespace "aerostream", set "log", key "{stream}:{partition}:{offset}".
 *
 * Phase-3 known limitations:
 *  - Offset counters restart from 0 on server restart (config + offsets are
 *    in-memory only; reconstruction deferred until config is persisted).
 *  - Headers (hdrs bin) not written; headers_count > 0 is silently ignored.
 */

#pragma once

#include <stdbool.h>

#include "base/proto.h"
#include "base/transaction.h"
#include "modules/aerostream/as_stream_replay.h"

/*
 * Initialise log module: create state hash, check namespace exists.
 * Called from as_stream_module_init().
 */
void as_stream_log_module_init(void);

/*
 * Handle an incoming STREAM_PRODUCE message. Takes ownership of nothing
 * (proto is owned by as_stream_dispatch). Returns:
 *   true  - rearm fd now (ack_mode == 0: fire-and-forget, already queued)
 *   false - done_cb will rearm fd after the storage write completes
 *           (ack_mode > 0) or force-closed fd on error.
 */
bool as_stream_log_handle_produce(as_file_handle *fd_h, as_proto *proto);

/*
 * Handle STREAM_CONSUME. Registers a push session for the consumer group,
 * signals the push loop, then returns true (rearm fd so ACKs can arrive).
 */
bool as_stream_log_handle_consume(as_file_handle *fd_h, as_proto *proto);

/*
 * Handle STREAM_ACK. Commits the group offset, decrements in_flight,
 * and wakes the push loop. Returns true (rearm for next ACK).
 */
bool as_stream_log_handle_ack(as_file_handle *fd_h, as_proto *proto);

/*
 * Handle STREAM_SEEK. Resolves the seek position (including binary search for
 * SEEK_TIMESTAMP), updates session->next_offset and resets in_flight under
 * push_lock, then signals the push loop to resume delivery.
 * Returns true (rearm — no wire response is sent for SEEK).
 */
bool as_stream_log_handle_seek(as_file_handle *fd_h, as_proto *proto);

/*
 * Handle STREAM_UNSUB (unsub_type == 0x00, consume session).
 * Marks all sessions for (stream, fd_h) as inactive across all partitions.
 * The push loop reaps them on its next pass.
 * Returns true (rearm).
 */
bool as_stream_log_handle_unsub(as_file_handle *fd_h, as_proto *proto);

/*
 * Build and send a STREAM_RECORD to a single fd_h.
 * correlation_id is in host byte order — converted to big-endian on the wire.
 * Returns true on success, false if the socket write failed.
 * Used by as_stream_pubsub for fan-out delivery.
 */
bool as_stream_log_send_record(as_file_handle *fd_h, uint64_t correlation_id,
		uint32_t partition_id, int64_t offset, int64_t ts_ns,
		const uint8_t *payload, uint32_t payload_sz);
