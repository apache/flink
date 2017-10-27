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

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.group.ChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.group.ChannelGroupFuture;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

import javax.annotation.Nonnull;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Routes messages to channels based on a routing key.
 * @param <K> the key type.
 */
public class KeyedChannelRouter<K> {

	private final AttributeKey<K> attributeKey;

	private final ChannelGroup channels;

	public KeyedChannelRouter(AttributeKey<K> attributeKey, ChannelGroup channelGroup) {
		this.attributeKey = checkNotNull(attributeKey);
		this.channels = checkNotNull(channelGroup);
	}

	/**
	 * Registers a channel to receive messages for a given routing key.
	 *
	 * @param channel the channel to register.
	 */
	public void register(@Nonnull Channel channel, @Nonnull K routingKey) {
		channel.attr(attributeKey).set(routingKey);
		channels.add(channel);
	}

	/**
	 * Unregisters a channel.
	 *
	 * @param channel the channel to unregister.
	 */
	public void unregister(@Nonnull Channel channel) {
		channels.remove(channel);
	}

	/**
	 * Writes and flushes an object to select channels based on a routing key.
	 *
	 * @param routingKey the key to select the target channel(s).
	 * @param o the object to write and flush.
	 */
	public ChannelGroupFuture write(@Nonnull K routingKey, @Nonnull Object o) {
		return channels.write(o, channel -> isMatch(channel, routingKey));
	}

	/**
	 * Writes and flushes an object to select channels based on a routing key.
	 *
	 * @param routingKey the key to select the target channel(s).
	 * @param o the object to write and flush.
	 */
	public ChannelGroupFuture writeAndFlush(@Nonnull K routingKey, @Nonnull Object o) {
		return channels.writeAndFlush(o, channel -> isMatch(channel, routingKey));
	}

	/**
	 * Closes all channels associated with the given routing key.
	 */
	public ChannelGroupFuture close(@Nonnull K routingKey) {
		return channels.close(channel -> isMatch(channel, routingKey));
	}

	private boolean isMatch(Channel channel, K routingKey) {
		return Objects.equals(routingKey, channel.attr(attributeKey).get());
	}
}
