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

package org.apache.flink.runtime.rest.websocket;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;

/**
 * A WebSocket for sending and receiving messages.
 *
 * @param <I> type of the server-to-client messages.
 * @param <O> type of the client-to-server messages.
 */
public interface WebSocket<I extends ResponseBody, O extends RequestBody> {

	/**
	 * Adds a listener for websocket messages.
	 */
	void addListener(WebSocketListener<I> listener);

	/**
	 * Sends a message.
	 */
	ChannelFuture send(O message);

	/**
	 * Closes the websocket.
	 */
	ChannelFuture close();
}
