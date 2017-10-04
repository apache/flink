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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import com.fasterxml.jackson.annotation.JsonIgnore;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link ResponseBody} implementation for a WebSocket upgrade response.
 *
 * <p>This response type is not actually sent to the client, rather it triggers an
 * actual WebSocket handshake response, and then registers the contained channel handler
 * for subsequent message processing.
 */
public class WebSocketUpgradeResponseBody implements ResponseBody {

	private final ChannelHandler channelHandler;

	public WebSocketUpgradeResponseBody(ChannelHandler channelHandler) {
		this.channelHandler = checkNotNull(channelHandler);
	}

	@JsonIgnore
	public ChannelHandler getChannelHandler() {
		return channelHandler;
	}
}
