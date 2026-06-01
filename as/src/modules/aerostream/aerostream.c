/*
 * aerostream.c
 *
 * AeroStream module entry point: dispatch and phase-1 stubs.
 * Each stub echoes the correlation_id back with AS_STREAM_ERR_NOT_FOUND
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
seek_type_name(uint8_t seek_type)
{
	switch (seek_type) {
	case AS_STREAM_SEEK_LATEST:    return "LATEST";
	case AS_STREAM_SEEK_EARLIEST:  return "EARLIEST";
	case AS_STREAM_SEEK_OFFSET:    return "OFFSET";
	case AS_STREAM_SEEK_TIMESTAMP: return "TIMESTAMP";
	default:                       return "unknown";
	}
}

static const char *
status_name(uint8_t status)
{
	switch (status) {
	case AS_STREAM_OK:                  return "OK";
	case AS_STREAM_ERR_NOT_FOUND:       return "NOT_FOUND";
	case AS_STREAM_ERR_STORAGE:         return "STORAGE";
	case AS_STREAM_ERR_OOO_ACK:         return "OOO_ACK";
	case AS_STREAM_ERR_MAX_IN_FLIGHT:   return "MAX_IN_FLIGHT";
	case AS_STREAM_ERR_INVALID_SEEK:    return "INVALID_SEEK";
	case AS_STREAM_ERR_GROUP_NOT_FOUND: return "GROUP_NOT_FOUND";
	case AS_STREAM_ERR_AUTH:            return "AUTH";
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
			"aerostream: PRODUCE enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, proto->sz);

	if (proto->sz < sizeof(as_stream_produce_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: PRODUCE too short - got %lu need %zu fd %d client %s",
				proto->sz, sizeof(as_stream_produce_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AS_STREAM_ERR_NOT_FOUND);
	}

	as_stream_produce_msg *msg = (as_stream_produce_msg *)proto->body;

	uint64_t corr_id    = cf_swap_from_be64(msg->hdr.correlation_id);
	uint16_t hdr_count  = cf_swap_from_be16(msg->rec_hdr.headers_count);
	uint32_t payload_sz = cf_swap_from_be32(msg->rec_hdr.payload_size);

	cf_debug(AS_SERVICE,
			"aerostream: PRODUCE stream=%.63s corr_id %lu partition_key=%.63s "
			"ack_mode %u headers %u payload_sz %u fd %d client %s [stub]",
			msg->hdr.stream_name, corr_id,
			msg->partition_key,
			msg->ack_mode,
			hdr_count, payload_sz,
			CSFD(&fd_h->sock), fd_h->client);

	bool ok = send_err(fd_h, msg->hdr.correlation_id, AS_STREAM_ERR_NOT_FOUND);

	cf_debug(AS_SERVICE,
			"aerostream: PRODUCE exit stream=%.63s corr_id %lu send_ok %d",
			msg->hdr.stream_name, corr_id, (int)ok);

	return ok;
}

static bool
handle_consume(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: CONSUME enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, proto->sz);

	if (proto->sz < sizeof(as_stream_consume_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: CONSUME too short - got %lu need %zu fd %d client %s",
				proto->sz, sizeof(as_stream_consume_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AS_STREAM_ERR_NOT_FOUND);
	}

	as_stream_consume_msg *msg = (as_stream_consume_msg *)proto->body;

	uint64_t corr_id      = cf_swap_from_be64(msg->hdr.correlation_id);
	uint32_t partition_id = cf_swap_from_be32(msg->partition_id);
	int64_t  seek_val     = (int64_t)cf_swap_from_be64((uint64_t)msg->seek_value);
	uint32_t max_flight   = cf_swap_from_be32(msg->max_in_flight);

	if (partition_id == 0xFFFFFFFF) {
		cf_debug(AS_SERVICE,
				"aerostream: CONSUME stream=%.63s group=%.63s corr_id %lu "
				"partition=ALL seek_type %s seek_val %ld max_in_flight %u "
				"fd %d client %s [stub]",
				msg->hdr.stream_name, msg->group_name, corr_id,
				seek_type_name(msg->seek_type), seek_val, max_flight,
				CSFD(&fd_h->sock), fd_h->client);
	}
	else {
		cf_debug(AS_SERVICE,
				"aerostream: CONSUME stream=%.63s group=%.63s corr_id %lu "
				"partition %u seek_type %s seek_val %ld max_in_flight %u "
				"fd %d client %s [stub]",
				msg->hdr.stream_name, msg->group_name, corr_id,
				partition_id,
				seek_type_name(msg->seek_type), seek_val, max_flight,
				CSFD(&fd_h->sock), fd_h->client);
	}

	/* phase-4: will take ownership of fd_h and return false without send_err */
	bool ok = send_err(fd_h, msg->hdr.correlation_id, AS_STREAM_ERR_NOT_FOUND);

	cf_debug(AS_SERVICE,
			"aerostream: CONSUME exit stream=%.63s group=%.63s corr_id %lu "
			"send_ok %d",
			msg->hdr.stream_name, msg->group_name, corr_id, (int)ok);

	return ok;
}

static bool
handle_ack(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: ACK enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, proto->sz);

	if (proto->sz < sizeof(as_stream_ack_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: ACK too short - got %lu need %zu fd %d client %s",
				proto->sz, sizeof(as_stream_ack_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AS_STREAM_ERR_NOT_FOUND);
	}

	as_stream_ack_msg *msg = (as_stream_ack_msg *)proto->body;

	uint64_t corr_id      = cf_swap_from_be64(msg->hdr.correlation_id);
	uint32_t partition_id = cf_swap_from_be32(msg->partition_id);
	int64_t  offset       = (int64_t)cf_swap_from_be64((uint64_t)msg->offset);

	cf_debug(AS_SERVICE,
			"aerostream: ACK stream=%.63s group=%.63s corr_id %lu "
			"partition %u offset %ld fd %d client %s [stub]",
			msg->hdr.stream_name, msg->group_name, corr_id,
			partition_id, offset,
			CSFD(&fd_h->sock), fd_h->client);

	bool ok = send_err(fd_h, msg->hdr.correlation_id, AS_STREAM_ERR_NOT_FOUND);

	cf_debug(AS_SERVICE,
			"aerostream: ACK exit stream=%.63s offset %ld send_ok %d",
			msg->hdr.stream_name, offset, (int)ok);

	return ok;
}

static bool
handle_seek(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: SEEK enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, proto->sz);

	if (proto->sz < sizeof(as_stream_seek_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: SEEK too short - got %lu need %zu fd %d client %s",
				proto->sz, sizeof(as_stream_seek_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AS_STREAM_ERR_NOT_FOUND);
	}

	as_stream_seek_msg *msg = (as_stream_seek_msg *)proto->body;

	uint64_t corr_id      = cf_swap_from_be64(msg->hdr.correlation_id);
	uint32_t partition_id = cf_swap_from_be32(msg->partition_id);
	int64_t  seek_val     = (int64_t)cf_swap_from_be64((uint64_t)msg->seek_value);

	cf_debug(AS_SERVICE,
			"aerostream: SEEK stream=%.63s group=%.63s corr_id %lu "
			"partition %u seek_type %s seek_val %ld fd %d client %s [stub]",
			msg->hdr.stream_name, msg->group_name, corr_id,
			partition_id,
			seek_type_name(msg->seek_type), seek_val,
			CSFD(&fd_h->sock), fd_h->client);

	bool ok = send_err(fd_h, msg->hdr.correlation_id, AS_STREAM_ERR_NOT_FOUND);

	cf_debug(AS_SERVICE,
			"aerostream: SEEK exit stream=%.63s partition %u seek_val %ld "
			"send_ok %d",
			msg->hdr.stream_name, partition_id, seek_val, (int)ok);

	return ok;
}

static bool
handle_sub(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: SUB enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, proto->sz);

	if (proto->sz < sizeof(as_stream_sub_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: SUB too short - got %lu need %zu fd %d client %s",
				proto->sz, sizeof(as_stream_sub_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AS_STREAM_ERR_NOT_FOUND);
	}

	as_stream_sub_msg *msg = (as_stream_sub_msg *)proto->body;

	uint64_t corr_id = cf_swap_from_be64(msg->hdr.correlation_id);

	cf_debug(AS_SERVICE,
			"aerostream: SUB stream=%.63s topic=%.63s corr_id %lu "
			"fd %d client %s [stub]",
			msg->hdr.stream_name, msg->topic, corr_id,
			CSFD(&fd_h->sock), fd_h->client);

	bool ok = send_err(fd_h, msg->hdr.correlation_id, AS_STREAM_ERR_NOT_FOUND);

	cf_debug(AS_SERVICE,
			"aerostream: SUB exit topic=%.63s corr_id %lu send_ok %d",
			msg->topic, corr_id, (int)ok);

	return ok;
}

static bool
handle_unsub(as_file_handle *fd_h, as_proto *proto)
{
	cf_debug(AS_SERVICE,
			"aerostream: UNSUB enter fd %d client %s proto_sz %lu",
			CSFD(&fd_h->sock), fd_h->client, proto->sz);

	if (proto->sz < sizeof(as_stream_unsub_msg)) {
		cf_warning(AS_SERVICE,
				"aerostream: UNSUB too short - got %lu need %zu fd %d client %s",
				proto->sz, sizeof(as_stream_unsub_msg),
				CSFD(&fd_h->sock), fd_h->client);
		return send_err(fd_h, 0, AS_STREAM_ERR_NOT_FOUND);
	}

	as_stream_unsub_msg *msg = (as_stream_unsub_msg *)proto->body;

	uint64_t corr_id    = cf_swap_from_be64(msg->hdr.correlation_id);
	const char *unsub_type_str = (msg->unsub_type == 0x00) ?
			"consume-session" : "pubsub-subscription";

	cf_debug(AS_SERVICE,
			"aerostream: UNSUB stream=%.63s corr_id %lu unsub_type %u (%s) "
			"fd %d client %s [stub]",
			msg->hdr.stream_name, corr_id,
			msg->unsub_type, unsub_type_str,
			CSFD(&fd_h->sock), fd_h->client);

	bool ok = send_err(fd_h, msg->hdr.correlation_id, AS_STREAM_ERR_NOT_FOUND);

	cf_debug(AS_SERVICE,
			"aerostream: UNSUB exit stream=%.63s unsub_type %s send_ok %d",
			msg->hdr.stream_name, unsub_type_str, (int)ok);

	return ok;
}

/*
 * Public API.
 */

void
as_stream_module_init(void)
{
	cf_info(AS_SERVICE, "aerostream: module init");
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
			proto->type, proto_type_name(proto->type), proto->sz,
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
