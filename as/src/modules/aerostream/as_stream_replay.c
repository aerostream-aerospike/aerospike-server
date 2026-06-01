/*
 * as_stream_replay.c
 *
 * Seek / replay: offset resolution including O(log N) timestamp binary search.
 */

#include "modules/aerostream/as_stream_replay.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "fabric/partition.h"
#include "storage/storage.h"

/* Match the key format used by the produce path. */
#define REPLAY_KEY_MAX_LEN  96   /* stream(63) + ":" + part(10) + ":" + off(20) */

/*
 * Read just the "ts" bin from a log record at the given offset.
 * Used by the binary search — reads only the ts particle, never allocates
 * the payload.
 *
 * Returns true and sets *out_ts if the record exists.
 * Returns false if the record is not found (not yet committed or evicted).
 */
static bool
read_record_ts(as_namespace *ns, const char *stream_name,
		uint32_t partition_id, int64_t offset, int64_t *out_ts)
{
	char key_str[REPLAY_KEY_MAX_LEN];
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

	if (as_storage_record_load_bins(&rd) == 0) {
		as_bin *b = as_bin_get(&rd, "ts");
		if (b != NULL) {
			*out_ts = as_bin_particle_integer_value(b);
			ok = true;
		}
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);
	as_partition_release(&rsv);

	return ok;
}

/*
 * Binary search for the first offset whose committed timestamp >= target_ts.
 *
 * Timestamps are server-assigned monotonically with offset within a partition,
 * so bisection always converges.  Each probe is one direct key read — O(log N)
 * total, no secondary index.
 *
 * If every record has ts < target_ts, returns head_offset + 1 (consumer waits
 * for the next produce).  If no records exist, returns 0.
 */
static int64_t
seek_by_timestamp(as_namespace *ns, const char *stream_name,
		uint32_t partition_id, uint64_t target_ts, int64_t head_offset)
{
	if (head_offset < 0) {
		cf_debug(AS_SERVICE,
				"as_stream_replay: ts seek on empty partition "
				"stream=%.63s part %u — returning 0",
				stream_name, partition_id);
		return 0;
	}

	/*
	 * Exclusive upper bound: lo is in [0, head_offset + 1].
	 * When the loop exits with lo = head_offset + 1, every record had
	 * ts < target — the consumer should wait for the next produce.
	 * When lo <= head_offset, lo is the first offset with ts >= target.
	 */
	int64_t lo = 0;
	int64_t hi = head_offset + 1;  /* exclusive upper bound */

	cf_debug(AS_SERVICE,
			"as_stream_replay: ts seek stream=%.63s part %u "
			"target_ts %lu head %ld",
			stream_name, partition_id, target_ts, head_offset);

	while (lo < hi) {
		int64_t mid    = lo + (hi - lo) / 2;
		int64_t ts     = 0;
		bool    found  = read_record_ts(ns, stream_name, partition_id,
				mid, &ts);

		cf_debug(AS_SERVICE,
				"as_stream_replay: bisect lo %ld mid %ld hi %ld "
				"ts %ld found %d",
				lo, mid, hi, ts, (int)found);

		if (!found) {
			/*
			 * Record not found: either not yet committed (produce in flight)
			 * or evicted by TTL.  Treat as ts = 0 (effectively "before
			 * target") so we advance lo past it.
			 */
			lo = mid + 1;
			continue;
		}

		if ((uint64_t)ts < target_ts) {
			lo = mid + 1;
		}
		else {
			hi = mid;
		}
	}

	/*
	 * lo == hi.  If every record had ts < target, lo = head_offset + 1 and
	 * the consumer will wait for the next produce.  Otherwise lo is the first
	 * record with ts >= target.
	 */
	cf_debug(AS_SERVICE,
			"as_stream_replay: ts seek result offset %ld "
			"stream=%.63s part %u",
			lo, stream_name, partition_id);

	return lo;
}

/*
 * Public API.
 */

void
as_stream_replay_module_init(void)
{
	cf_info(AS_SERVICE, "as_stream_replay: init");
}

int64_t
as_stream_replay_resolve(as_namespace *ns, const char *stream_name,
		uint32_t partition_id, uint8_t seek_type, int64_t seek_value,
		int64_t head_offset)
{
	cf_debug(AS_SERVICE,
			"as_stream_replay: resolve stream=%.63s part %u "
			"seek_type %u seek_val %ld head %ld",
			stream_name, partition_id, seek_type, seek_value, head_offset);

	int64_t result;

	switch (seek_type) {

	case AS_STREAM_SEEK_EARLIEST:
		result = 0;
		break;

	case AS_STREAM_SEEK_LATEST:
		/*
		 * Start after the last produced record.  head_offset + 1 = offset_seq
		 * (next offset to be assigned).  If no records exist yet, start at 0.
		 */
		result = (head_offset < 0) ? 0 : head_offset + 1;
		break;

	case AS_STREAM_SEEK_OFFSET:
		if (seek_value < 0) {
			cf_warning(AS_SERVICE,
					"as_stream_replay: SEEK_OFFSET negative value %ld "
					"stream=%.63s part %u — clamping to 0",
					seek_value, stream_name, partition_id);
			result = 0;
		}
		else {
			/* Seeking beyond head_offset is valid: consumer waits. */
			result = seek_value;
		}
		break;

	case AS_STREAM_SEEK_TIMESTAMP:
		if (ns == NULL) {
			cf_warning(AS_SERVICE,
					"as_stream_replay: SEEK_TIMESTAMP with NULL namespace "
					"stream=%.63s part %u — falling back to EARLIEST",
					stream_name, partition_id);
			result = 0;
		}
		else {
			result = seek_by_timestamp(ns, stream_name, partition_id,
					(uint64_t)seek_value, head_offset);
		}
		break;

	default:
		cf_warning(AS_SERVICE,
				"as_stream_replay: unknown seek_type %u stream=%.63s "
				"part %u — defaulting to EARLIEST",
				seek_type, stream_name, partition_id);
		result = 0;
		break;
	}

	cf_debug(AS_SERVICE,
			"as_stream_replay: resolved stream=%.63s part %u "
			"seek_type %u → offset %ld",
			stream_name, partition_id, seek_type, result);

	return result;
}
