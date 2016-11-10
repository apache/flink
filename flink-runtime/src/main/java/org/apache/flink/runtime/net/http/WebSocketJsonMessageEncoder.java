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

package org.apache.flink.runtime.net.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converts user-defined objects to WebSocket messages.
 */
@ChannelHandler.Sharable
public class WebSocketJsonMessageEncoder<T> extends MessageToMessageEncoder<T> {

	private static final Logger LOG = LoggerFactory.getLogger(WebSocketJsonMessageEncoder.class);

	private final ObjectMapper mapper;
	private final Class<T> valueType;

	/**
	 * Construct an encoder to write WebSocket messages using the given mapper and value type.
	 * @param mapper the mapper to deserialize the message with.
	 * @param valueType the base type of the message.
     */
	public WebSocketJsonMessageEncoder(ObjectMapper mapper, Class<T> valueType) {
		super(valueType);
		this.mapper = checkNotNull(mapper);
		this.valueType = checkNotNull(valueType);
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, T msg, List<Object> out) throws Exception {
		String value;
		try {
			value = mapper.writeValueAsString(msg);
		}
		catch(Exception ex) {
			throw new IOException("Unable to encode WebSocket message", ex);
		}
		out.add(new TextWebSocketFrame(value));
	}
}
