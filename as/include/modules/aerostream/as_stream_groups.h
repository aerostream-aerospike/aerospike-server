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
 * Writes use fire-and-forget IOPS so the ACK handler is not blocked.
 *
 * Phase-4 limitation: no CAS on commit — concurrent consumer instances on the
 * same group+partition can double-commit. CAS via as_operate deferred to
 * phase-5.
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
