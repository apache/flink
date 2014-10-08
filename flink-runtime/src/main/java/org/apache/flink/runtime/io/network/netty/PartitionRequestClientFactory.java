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
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for {@link PartitionRequestClient} instances.
 * <p>
 * Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 */
class PartitionRequestClientFactory {

	private final NettyClient nettyClient;

	private final ConcurrentMap<RemoteAddress, Object> clients = new ConcurrentHashMap<RemoteAddress, Object>();

	PartitionRequestClientFactory(NettyClient nettyClient) {
		this.nettyClient = nettyClient;
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link PartitionRequestClient} instance for this connection.
	 */
	PartitionRequestClient createPartitionRequestClient(RemoteAddress remoteAddress) throws IOException {
		final Object entry = clients.get(remoteAddress);

		final PartitionRequestClient client;
		if (entry != null) {
			// Existing channel or connecting channel
			if (entry instanceof PartitionRequestClient) {
				client = (PartitionRequestClient) entry;
			}
			else {
				ConnectingChannel future = (ConnectingChannel) entry;
				client = future.waitForChannel();
			}
		}
		else {
			// No channel yet. Create one, but watch out for a race.
			// We create a "connecting future" and atomically add it to the map.
			// Only the thread that really added it establishes the channel.
			// The others need to wait on that original establisher's future.
			ConnectingChannel connectingChannel = new ConnectingChannel(remoteAddress, this);
			Object old = clients.putIfAbsent(remoteAddress, connectingChannel);

			if (old == null) {
				nettyClient.connect(remoteAddress.getAddress()).addListener(connectingChannel);

				client = connectingChannel.waitForChannel();

				Object previous = clients.put(remoteAddress, client);

				if (connectingChannel != previous) {
					throw new IOException("Race condition while establishing channel connection.");
				}
			}
			else if (old instanceof ConnectingChannel) {
				client = ((ConnectingChannel) old).waitForChannel();
			}
			else {
				client = (PartitionRequestClient) old;
			}
		}

		// Make sure to increment the reference count before handing a client
		// out to ensure correct bookkeeping for channel closing.
		if (client.incrementReferenceCounter()) {
			return client;
		}
		else {
			// There was a race with a close, try again.
			destroyPartitionRequestClient(remoteAddress, client);

			return createPartitionRequestClient(remoteAddress);
		}
	}

	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link RemoteAddress}.
	 */
	void destroyPartitionRequestClient(RemoteAddress remoteAddress, PartitionRequestClient client) {
		clients.remove(remoteAddress, client);
	}

	private static final class ConnectingChannel implements ChannelFutureListener {

		private final Object connectLock = new Object();

		private final RemoteAddress remoteAddress;

		private final PartitionRequestClientFactory clientFactory;

		public ConnectingChannel(RemoteAddress remoteAddress, PartitionRequestClientFactory clientFactory) {
			this.remoteAddress = remoteAddress;
			this.clientFactory = clientFactory;
		}

		private void handInChannel(Channel channel) {
			synchronized (connectLock) {
				PartitionRequestClientHandler requestHandler =
						(PartitionRequestClientHandler) channel.pipeline().get(PartitionRequestProtocol.CLIENT_REQUEST_HANDLER_NAME);

				partitionRequestClient = new PartitionRequestClient(channel, requestHandler, remoteAddress, clientFactory);
				connectLock.notifyAll();
			}
		}

		private volatile PartitionRequestClient partitionRequestClient;

		private volatile Throwable error;

		private PartitionRequestClient waitForChannel() throws IOException {
			synchronized (connectLock) {
				while (error == null && partitionRequestClient == null) {
					try {
						connectLock.wait(2000);
					}
					catch (InterruptedException e) {
						throw new RuntimeException("Wait for channel connection interrupted.");
					}
				}
			}

			if (error != null) {
				throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
			}

			return partitionRequestClient;
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
				notifyOfError(new IllegalStateException("Connecting the channel has been cancelled."));
			}
		}
	}
}
