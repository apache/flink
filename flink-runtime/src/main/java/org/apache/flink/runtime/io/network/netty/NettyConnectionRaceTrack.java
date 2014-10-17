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

package org.apache.flink.runtime.io.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.flink.runtime.io.network.RemoteReceiver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NettyConnectionRaceTrack {

	private final NettyClient client;

	private final ConcurrentMap<RemoteReceiver, Object> channels = new ConcurrentHashMap<RemoteReceiver, Object>();

	public NettyConnectionRaceTrack(NettyClient client) {
		this.client = client;
	}

	Channel connectOrGet(RemoteReceiver target) throws IOException {
		final Object entry = channels.get(target);

		final Channel channel;
		if (entry != null) {
			// existing channel or channel in buildup
			if (entry instanceof Channel) {
				channel = (Channel) entry;
			}
			else {
				ConnectingChannel future = (ConnectingChannel) entry;
				channel = future.waitForChannel();
			}
		}
		else {
			// No channel yet. Create one, but watch out for a race.
			// We create a "buildup future" and atomically add it to the map.
			// Only the thread that really added it establishes the channel.
			// The others need to wait on that original establisher's future.
			ConnectingChannel connectingChannel = new ConnectingChannel();
			Object old = channels.putIfAbsent(target, connectingChannel);

			if (old == null) {

				client.connect(target.getConnectionAddress()).addListener(connectingChannel);

				channel = connectingChannel.waitForChannel();

				Object previous = channels.put(target, channel);

				if (connectingChannel != previous) {
					throw new IOException("Race condition during channel build up.");
				}
			}
			else if (old instanceof ConnectingChannel) {
				channel = ((ConnectingChannel) old).waitForChannel();
			}
			else {
				channel = (Channel) old;
			}
		}

		if (!channel.isActive()) {
			channels.remove(target, channel);
			return connectOrGet(target);
		}

		return channel;
	}

	private static final class ConnectingChannel implements ChannelFutureListener {

		private final Object connectLock = new Object();

		private volatile Channel channel;

		private volatile Throwable error;

		private void handInChannel(Channel channel) {
			synchronized (connectLock) {
				this.channel = channel;
				connectLock.notifyAll();
			}
		}

		private Channel waitForChannel() throws IOException {
			synchronized (connectLock) {
				while (error == null && channel == null) {
					try {
						connectLock.wait(2000);
					} catch (InterruptedException e) {
						throw new RuntimeException("Wait for channel connection interrupted.");
					}
				}
			}

			if (error != null) {
				throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
			}

			return channel;
		}

		private void notifyOfError(Throwable error) {
			synchronized (connectLock) {
				this.error = error;
				connectLock.notifyAll();
			}
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				handInChannel(future.channel());
			}
			else if (future.cause() != null) {
				notifyOfError(future.cause());
			}
			else {
				notifyOfError(new Exception("Connecting the channel failed."));
			}
		}
	}
}
