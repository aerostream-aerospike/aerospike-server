/*
 * aerostream.h
 *
 * Public API for the AeroStream module. Called only from service.c dispatch.
 * All implementation lives in as/src/modules/aerostream/.
 */

#pragma once

#include "base/proto.h"
#include "base/transaction.h"
#include "modules/aerostream/as_stream_config.h"
#include "modules/aerostream/as_stream_groups.h"
#include "modules/aerostream/as_stream_log.h"
#include "modules/aerostream/as_stream_pubsub.h"
#include "modules/aerostream/as_stream_replay.h"

/*
 * Called once at server startup, before any connections are accepted.
 * Initialises in-memory state (config cache, stream states, push threads).
 */
void as_stream_module_init(void);

/*
 * Called at server shutdown. Stops push threads, frees module state.
 */
void as_stream_module_shutdown(void);

/*
 * Entry point from service.c. Routes proto to the appropriate handler.
 * Takes ownership of proto (frees it). Rearms fd_h unless the message
 * opens a persistent push session (STREAM_CONSUME).
 */
void as_stream_dispatch(as_file_handle *fd_h, as_proto *proto);
