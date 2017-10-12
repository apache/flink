/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;

/**
 * Extended REST handler specification with websocket information.
 *
 * <p>Implementations must be state-less.
 *
 * @param <M> message parameters type
 * @param <I> inbound message type
 * @param <O> outbound message type
 */
public interface WebSocketSpecification<M extends MessageParameters, I extends RequestBody, O extends ResponseBody> extends RestHandlerSpecification {

	@Override
	default HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	/**
	 * Returns the WebSocket subprotocol associated with the REST resource.
	 */
	String getSubprotocol();

	/**
	 * Returns the base class of client-to-server messages.
	 *
	 * @return class of the message
	 */
	Class<I> getClientClass();

	/**
	 * Returns the base class of server-to-client messages.
	 *
	 * @return class of the message
	 */
	Class<O> getServerClass();

	/**
	 * Returns a new {@link MessageParameters} object.
	 *
	 * @return new message parameters object
	 */
	M getUnresolvedMessageParameters();
}
