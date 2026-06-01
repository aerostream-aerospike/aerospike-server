/*
 * aerostream.c
 *
 * AeroStream module entry point: dispatch and phase-1 stubs.
 * Each stub echoes the correlation_id back with AEROSTREAM_ERR_NOT_FOUND
 * so a client can confirm dispatch is wired correctly.
 *
 * Handlers return true  -> caller frees proto + rearms fd (request/response).
 *              return false -> handler keeps fd (persistent push session).
 * Phase-1: all handlers return true.
 */

#include "modules/aerostream/aerostream.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "log.h"
#include "socket.h"

#include "base/proto.h"
#include "base/transaction.h"

/*
 * Debug helpers.
 */

static const char *
status_name(uint8_t status)
{
	switch (status) {
	case AEROSTREAM_OK:                  return "OK";
	case AEROSTREAM_ERR_NOT_FOUND:       return "NOT_FOUND";
	case AEROSTREAM_ERR_STORAGE:         return "STORAGE";
	case AEROSTREAM_ERR_OOO_ACK:         return "OOO_ACK";
	case AEROSTREAM_ERR_MAX_IN_FLIGHT:   return "MAX_IN_FLIGHT";
	case AEROSTREAM_ERR_INVALID_SEEK:    return "INVALID_SEEK";
	case AEROSTREAM_ERR_GROUP_NOT_FOUND: return "GROUP_NOT_FOUND";
	case AEROSTREAM_ERR_AUTH:            return "AUTH";
	default:                            return "unknown";
	}
}

static const char *
proto_type_name(uint8_t type)
{
	switch (type) {
	case AS_PROTO_TYPE_STREAM_PRODUCE:  return "STREAM_PRODUCE";
	case AS_PROTO_TYPE_STREAM_PROD_ACK: return "STREAM_PROD_ACK";
	case AS_PROTO_TYPE_STREAM_CONSUME:  return "STREAM_CONSUME";
	case AS_PROTO_TYPE_STREAM_RECORD:   return "STREAM_RECORD";
	case AS_PROTO_TYPE_STREAM_ACK:      return "STREAM_ACK";
	case AS_PROTO_TYPE_STREAM_SEEK:     return "STREAM_SEEK";
	case AS_PROTO_TYPE_STREAM_SUB:      return "STREAM_SUB";
	case AS_PROTO_TYPE_STREAM_UNSUB:    return "STREAM_UNSUB";
	default:                            return "unknown";
	}
}

/*
 * Local helpers.
 */

/* Returns true if the send succeeded, false if it failed and force-closed fd. */
static bool
send_err(as_file_handle *fd_h, uint64_t correlation_id, uint8_t status)
{
	struct __attribute__((packed)) {
		as_proto               proto;
		as_stream_prod_ack_msg ack;
	} msg;

	msg.proto.version = PROTO_VERSION;
	msg.proto.type    = AS_PROTO_TYPE_STREAM_PROD_ACK;
	msg.proto.sz      = sizeof(as_stream_prod_ack_msg);
	as_proto_swap(&msg.proto);

	/* body fields sent as-is for now: phase-3 will add proper be-swaps */
	msg.ack.correlation_id = correlation_id;
	msg.ack.offset         = -1;
	msg.ack.partition_id   = 0;
	msg.ack.timestamp_ns   = 0;
	msg.ack.status         = status;

	cf_debug(AS_SERVICE,
			"aerostream: send_err -> fd %d client %s corr_id %lu status %u (%s) "
			"resp_sz %zu",
			CSFD(&fd_h->sock), fd_h->client,
			cf_swap_from_be64(correlation_id),
			status, status_name(status),
			sizeof(msg));

	if (cf_socket_send_all(&fd_h->sock, &msg, sizeof(msg), MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		cf_warning(AS_SERVICE,
				"aerostream: send_err write failed fd %d client %s errno %d - "
				"force closing",
				CSFD(&fd_h->sock), fd_h->client, errno);
		as_end_of_transaction_force_close(fd_h);
		return false;
	}

	cf_debug(AS_SERVICE, "aerostream: send_err write ok fd %d client %s",
			CSFD(&fd_h->sock), fd_h->client);

	return true;
}

/*
 * Stub handlers - one per incoming message type.
 */

static bool
handle_produce(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: PRODUCE delegating to as_stream_log fd %d client %s",
			CSFD(&fd_h->sock), fd_h->client);
	return as_stream_log_handle_produce(fd_h, proto);
}

static bool
handle_consume(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: CONSUME delegating to as_stream_log fd %d client %s",
			CSFD(&fd_h->sock), fd_h->client);
	return as_stream_log_handle_consume(fd_h, proto);
}

static bool
handle_ack(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: ACK delegating to as_stream_log fd %d client %s",
			CSFD(&fd_h->sock), fd_h->client);
	return as_stream_log_handle_ack(fd_h, proto);
}

static bool
handle_seek(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: SEEK delegating to as_stream_log fd %d client %s",
			CSFD(&fd_h->sock), fd_h->client);
	return as_stream_log_handle_seek(fd_h, proto);
}

static bool
handle_sub(as_file_handle *fd_h, as_proto *proto)
{
	if (proto->sz < sizeof(as_stream_sub_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: SUB too short got %lu need %zu fd %d client %s",
				(uint64_t)proto->sz, sizeof(as_stream_sub_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AEROSTREAM_ERR_NOT_FOUND);
	}

	as_stream_sub_msg *msg = (as_stream_sub_msg *)proto->body;

	uint64_t corr_id = cf_swap_from_be64(msg->hdr.correlation_id);

	cf_debug(AS_SERVICE,
			"aerostream: SUB topic=%.63s corr_id %lu fd %d client %s",
			msg->topic, corr_id, CSFD(&fd_h->sock), fd_h->client);

	as_stream_pubsub_subscribe(fd_h, corr_id, (const char *)msg->topic);

	cf_info(AS_SERVICE,
			"aerostream: SUB registered topic=%.63s fd %d client %s",
			msg->topic, CSFD(&fd_h->sock), fd_h->client);

	/* No wire response for SUB — delivery starts immediately on next produce. */
	return true;
}

static bool
handle_unsub(as_file_handle *fd_h, as_proto *proto)
{
	if (proto->sz < sizeof(as_stream_unsub_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: UNSUB too short - got %lu need %zu fd %d client %s",
				(uint64_t)proto->sz, sizeof(as_stream_unsub_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AEROSTREAM_ERR_NOT_FOUND);
	}

	as_stream_unsub_msg *msg = (as_stream_unsub_msg *)proto->body;

	if (msg->unsub_type == 0x00) {
		cf_debug(AS_SERVICE,
				"aerostream: UNSUB consume-session delegating fd %d client %s",
				CSFD(&fd_h->sock), fd_h->client);
		return as_stream_log_handle_unsub(fd_h, proto);
	}

	/* unsub_type == 0x01 — remove pub/sub subscription. */
	cf_debug(AS_SERVICE,
			"aerostream: UNSUB pubsub stream=%.63s fd %d client %s",
			msg->hdr.stream_name, CSFD(&fd_h->sock), fd_h->client);

	/*
	 * STREAM_UNSUB for pub/sub uses hdr.stream_name as the topic key,
	 * matching how STREAM_SUB's topic field is used during fan-out.
	 */
	as_stream_pubsub_unsubscribe(fd_h, (const char *)msg->hdr.stream_name);

	cf_info(AS_SERVICE,
			"aerostream: UNSUB pubsub removed topic=%.63s fd %d client %s",
			msg->hdr.stream_name, CSFD(&fd_h->sock), fd_h->client);

	return true;
}

/*
 * Public API.
 */

void
as_stream_module_init(void)
{
	cf_info(AS_SERVICE, "aerostream: module init");
	as_stream_config_module_init();
	as_stream_log_module_init();
}

void
as_stream_module_shutdown(void)
{
	cf_info(AS_SERVICE, "aerostream: module shutdown");
}

void
as_stream_dispatch(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: dispatch enter type %u (%s) proto_sz %lu fd %d client %s",
			proto->type, proto_type_name(proto->type), (uint64_t)proto->sz,
			CSFD(&fd_h->sock), fd_h->client);

	bool rearm = true;

	switch (proto->type) {
	case AS_PROTO_TYPE_STREAM_PRODUCE:
		rearm = handle_produce(fd_h, proto);
		break;
	case AS_PROTO_TYPE_STREAM_CONSUME:
		rearm = handle_consume(fd_h, proto);
		break;
	case AS_PROTO_TYPE_STREAM_ACK:
		rearm = handle_ack(fd_h, proto);
		break;
	case AS_PROTO_TYPE_STREAM_SEEK:
		rearm = handle_seek(fd_h, proto);
		break;
	case AS_PROTO_TYPE_STREAM_SUB:
		rearm = handle_sub(fd_h, proto);
		break;
	case AS_PROTO_TYPE_STREAM_UNSUB:
		rearm = handle_unsub(fd_h, proto);
		break;
	default:
		cf_warning(AS_SERVICE,
				"aerostream: unexpected type %u fd %d client %s",
				proto->type, CSFD(&fd_h->sock), fd_h->client);
		break;
	}

	cf_debug(AS_SERVICE,
			"aerostream: dispatch exit type %u (%s) rearm %d fd %d",
			proto->type, proto_type_name(proto->type), (int)rearm,
			CSFD(&fd_h->sock));

	cf_free(proto);

	if (rearm) {
		as_end_of_transaction_ok(fd_h);
	}
	/* rearm == false: either send_err already called force_close, or
	 * phase-4 push session took ownership of fd_h. Either way, do not touch fd. */
}
