/*
 * as_stream_groups.h
 *
 * Consumer group offset state: committed offset reads and writes against the
 * "consumer_offsets" set in the "aerostream" namespace.
 *
 * Key format: "{group}:{stream}:{partition_id}"
 * Bins:        committed (INTEGER), lag (INTEGER), updated_at (INTEGER)
 *
 * Reads use direct partition access (as_partition_reserve + as_record_get)
 * since they happen on the push loop thread which can block.
 *
 * Commits are CAS-protected: the ACK handler reads the current committed
 * offset + record generation, then issues a generation-checked write (or
 * create-only for the first commit). Concurrent consumers in the same group
 * racing to commit cannot double-commit — the loser's write fails with
 * AS_ERR_GENERATION and is dropped. The write itself is fire-and-forget IOPS.
 */

#pragma once

#include <stdint.h>

#define AS_STREAM_GROUPS_SET   "consumer_offsets"
#define AS_STREAM_GROUPS_NS    "aerostream"

/*
 * Initialise the groups module. Called from as_stream_module_init().
 * Currently a no-op placeholder for future per-module state.
 */
void as_stream_groups_module_init(void);

/*
 * Read the last committed offset for (group, stream, partition).
 * Returns -1 if no committed offset exists (new group).
 * Blocking — safe to call from the push loop thread.
 */
int64_t as_stream_groups_get_offset(const char *stream, const char *group,
		uint32_t partition_id);

/*
 * Fire-and-forget write of the new committed offset + lag.
 * lag = head_offset - new_offset.
 * Non-blocking — safe to call from any thread.
 */
void as_stream_groups_commit(const char *stream, const char *group,
		uint32_t partition_id, int64_t new_offset, int64_t lag);
