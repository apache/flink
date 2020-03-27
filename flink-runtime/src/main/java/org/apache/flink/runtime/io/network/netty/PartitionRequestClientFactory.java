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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for {@link NettyPartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 */
class PartitionRequestClientFactory {
	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientFactory.class);

	private final NettyClient nettyClient;

	private final int retryNumber;

	private final ConcurrentMap<ConnectionID, Object> clients = new ConcurrentHashMap<ConnectionID, Object>();

	PartitionRequestClientFactory(NettyClient nettyClient) {
		this(nettyClient, 0);
	}

	PartitionRequestClientFactory(NettyClient nettyClient, int retryNumber) {
		this.nettyClient = nettyClient;
		this.retryNumber = retryNumber;
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link NettyPartitionRequestClient} instance for this connection.
	 */
	NettyPartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) {
		NettyPartitionRequestClient client = null;
		while (client == null) {
			client = (NettyPartitionRequestClient) clients.computeIfAbsent(connectionId, unused -> {
				int tried = 0;
				while (true) {
					ConnectingChannel connectingChannel = null;
					try {
						connectingChannel = new ConnectingChannel(connectionId, this);
						nettyClient.connect(connectionId.getAddress()).addListener(connectingChannel);
						return connectingChannel.waitForChannel();
					} catch (IOException | InterruptedException | ChannelException e) {
						LOG.error("Failed {} times to connect to {}", tried, connectionId.getAddress(), e);
						if (++tried > retryNumber) {
							throw new CompletionException(e);
						}
					}
				}
			});

			if (!client.incrementReferenceCounter()) {
				destroyPartitionRequestClient(connectionId, client);
				client = null;
			}
		}

		return client;
	}

	public void closeOpenChannelConnections(ConnectionID connectionId) {
		Object entry = clients.get(connectionId);

		if (entry instanceof ConnectingChannel) {
			ConnectingChannel channel = (ConnectingChannel) entry;

			if (channel.dispose()) {
				clients.remove(connectionId, channel);
			}
		}
	}

	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link ConnectionID}.
	 */
	void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
		clients.remove(connectionId, client);
	}

	private static final class ConnectingChannel implements ChannelFutureListener {

		private final Object connectLock = new Object();

		private final ConnectionID connectionId;

		private final PartitionRequestClientFactory clientFactory;

		private boolean disposeRequestClient = false;

		public ConnectingChannel(ConnectionID connectionId, PartitionRequestClientFactory clientFactory) {
			this.connectionId = connectionId;
			this.clientFactory = clientFactory;
		}

		private boolean dispose() {
			boolean result;
			synchronized (connectLock) {
				if (partitionRequestClient != null) {
					result = partitionRequestClient.disposeIfNotUsed();
				}
				else {
					disposeRequestClient = true;
					result = true;
				}

				connectLock.notifyAll();
			}

			return result;
		}

		private void handInChannel(Channel channel) {
			synchronized (connectLock) {
				try {
					NetworkClientHandler clientHandler = channel.pipeline().get(NetworkClientHandler.class);
					partitionRequestClient = new NettyPartitionRequestClient(
						channel, clientHandler, connectionId, clientFactory);

					if (disposeRequestClient) {
						partitionRequestClient.disposeIfNotUsed();
					}

					connectLock.notifyAll();
				}
				catch (Throwable t) {
					notifyOfError(t);
				}
			}
		}

		private volatile NettyPartitionRequestClient partitionRequestClient;

		private volatile Throwable error;

		private NettyPartitionRequestClient waitForChannel() throws IOException, InterruptedException {
			synchronized (connectLock) {
				while (error == null && partitionRequestClient == null) {
					connectLock.wait(2000);
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
				notifyOfError(new RemoteTransportException(
						"Connecting to remote task manager + '" + connectionId.getAddress() +
								"' has failed. This might indicate that the remote task " +
								"manager has been lost.",
						connectionId.getAddress(), future.cause()));
			}
			else {
				notifyOfError(new LocalTransportException(
					String.format(
						"Connecting to remote task manager '%s' has been cancelled.",
						connectionId.getAddress()),
					null));
			}
		}
	}
}
