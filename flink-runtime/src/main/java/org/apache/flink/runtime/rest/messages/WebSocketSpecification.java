package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;

/**
 * Extended REST handler specification with websocket information.
 *
 * <p>Implementations must be state-less.
 *
 * @param <O> outbound message type
 * @param <I> inbound message type
 * @param <M> message parameters type
 */
public interface WebSocketSpecification<M extends MessageParameters, O extends RequestBody, I extends ResponseBody> extends RestHandlerSpecification {

	@Override
	default HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	/**
	 * Returns the WebSocket subprotocol associated with the REST resource.
	 */
	String getSubprotocol();

	/**
	 * Returns the base class of inbound messages.
	 *
	 * @return class of the response message
	 */
	Class<I> getInboundClass();

	/**
	 * Returns the base class of outbound messages.
	 *
	 * @return class of the request message
	 */
	Class<O> getOutboundClass();

	/**
	 * Returns a new {@link MessageParameters} object.
	 *
	 * @return new message parameters object
	 */
	M getUnresolvedMessageParameters();
}
