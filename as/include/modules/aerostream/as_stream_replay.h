/*
 * as_stream_replay.h
 *
 * Seek / replay logic: resolves any seek type to a concrete start offset.
 *
 * For SEEK_OFFSET and SEEK_EARLIEST / SEEK_LATEST the resolution is O(1).
 * For SEEK_TIMESTAMP the resolution is O(log N) direct key reads (binary
 * search on log record keys — timestamps are server-assigned monotonically
 * with offset within a partition, so bisection converges without a secondary
 * index scan).
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "base/datamodel.h"
#include "base/proto.h"

void as_stream_replay_module_init(void);

/*
 * Resolve a seek position to a concrete start offset.
 *
 * ns            — the aerostream namespace (may be NULL, returns 0)
 * stream_name   — stream to seek within
 * partition_id  — target partition
 * seek_type     — one of AS_STREAM_SEEK_*
 * seek_value    — offset (for SEEK_OFFSET) or timestamp_ns (SEEK_TIMESTAMP)
 * head_offset   — last assigned offset in this partition (-1 if no records)
 *
 * Return value: the offset the push loop should start delivering from.
 * Never returns a negative value — invalid / out-of-range inputs are clamped
 * to sensible defaults and logged.
 */
int64_t as_stream_replay_resolve(as_namespace *ns,
		const char *stream_name, uint32_t partition_id,
		uint8_t seek_type, int64_t seek_value, int64_t head_offset);
