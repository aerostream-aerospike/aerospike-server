/*
 * as_stream_config.h
 *
 * Per-stream configuration: partition count, TTL, ack mode.
 *
 * Phase-2: in-memory cache only (cf_shash, thread-safe).
 * Phase-3 TODO: persist config records to namespace "aerostream", set "config",
 * key = stream_name, bins n_parts/ttl_sec/ack_mode. Reload on module init using
 * the same storage-read pattern as offset-counter reconstruction.
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

/*
 * Stream name limits - match the wire struct (as_stream_hdr.stream_name[64]).
 */
#define AS_STREAM_NAME_MAX_LEN  63   /* max usable chars, excluding null */
#define AS_STREAM_NAME_SZ       64   /* buffer size including null terminator */

/*
 * Config defaults applied on first produce to a stream with no existing config.
 */
#define AS_STREAM_DEFAULT_NUM_PARTITIONS   8
#define AS_STREAM_DEFAULT_TTL_SECONDS      (7 * 24 * 3600)  /* 7 days */
#define AS_STREAM_DEFAULT_ACK_MODE         0x01             /* leader ack */

typedef struct {
	uint32_t num_partitions;
	uint32_t ttl_seconds;
	uint8_t  ack_mode;
} as_stream_config;

/*
 * Initialise the config cache. Called once from as_stream_module_init().
 */
void as_stream_config_module_init(void);

/*
 * Look up config for a named stream.
 * Returns true and fills *out_cfg if found in cache.
 * Returns false if the stream has no config yet.
 */
bool as_stream_config_get(const char *stream_name, as_stream_config *out_cfg);

/*
 * Write (create or update) config for a named stream.
 * Updates the in-memory cache. Storage persistence is phase-3.
 */
void as_stream_config_upsert(const char *stream_name,
		const as_stream_config *cfg);

/*
 * Return config for a named stream. If no config exists, populate with
 * defaults, upsert into cache, and return the defaults.
 * This is the hot path called by the produce handler.
 */
void as_stream_config_get_or_default(const char *stream_name,
		as_stream_config *out_cfg);
