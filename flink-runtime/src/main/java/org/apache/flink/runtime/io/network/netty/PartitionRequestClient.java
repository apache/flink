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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.AtomicDisposableReferenceCounter;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partition request client for remote partition requests.
 *
 * <p>This client is shared by all remote input channels, which request a partition
 * from the same {@link ConnectionID}.
 */
public class PartitionRequestClient {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClient.class);

	private final Channel tcpChannel;

	private final PartitionRequestClientHandler partitionRequestHandler;

	private final ConnectionID connectionId;

	private final PartitionRequestClientFactory clientFactory;

	/** If zero, the underlying TCP channel can be safely closed. */
	private final AtomicDisposableReferenceCounter closeReferenceCounter = new AtomicDisposableReferenceCounter();

	PartitionRequestClient(
			Channel tcpChannel,
			PartitionRequestClientHandler partitionRequestHandler,
			ConnectionID connectionId,
			PartitionRequestClientFactory clientFactory) {

		this.tcpChannel = checkNotNull(tcpChannel);
		this.partitionRequestHandler = checkNotNull(partitionRequestHandler);
		this.connectionId = checkNotNull(connectionId);
		this.clientFactory = checkNotNull(clientFactory);
	}

	boolean disposeIfNotUsed() {
		return closeReferenceCounter.disposeIfNotUsed();
	}

	/**
	 * Increments the reference counter.
	 * <p>
	 * Note: the reference counter has to be incremented before returning the
	 * instance of this client to ensure correct closing logic.
	 */
	boolean incrementReferenceCounter() {
		return closeReferenceCounter.increment();
	}

	/**
	 * Requests a remote intermediate result partition queue.
	 * <p>
	 * The request goes to the remote producer, for which this partition
	 * request client instance has been created.
	 */
	public ChannelFuture requestSubpartition(
			final ResultPartitionID partitionId,
			final int subpartitionIndex,
			final RemoteInputChannel inputChannel,
			int delayMs) throws IOException {

		checkNotClosed();

		LOG.debug("Requesting subpartition {} of partition {} with {} ms delay.",
				subpartitionIndex, partitionId, delayMs);

		partitionRequestHandler.addInputChannel(inputChannel);

		final PartitionRequest request = new PartitionRequest(
				partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());

		final ChannelFutureListener listener = new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
					partitionRequestHandler.removeInputChannel(inputChannel);
					inputChannel.onError(
							new LocalTransportException(
									"Sending the partition request failed.",
									future.channel().localAddress(), future.cause()
							));
				}
			}
		};

		if (delayMs == 0) {
			ChannelFuture f = tcpChannel.writeAndFlush(request);
			f.addListener(listener);
			return f;
		}
		else {
			final ChannelFuture[] f = new ChannelFuture[1];
			tcpChannel.eventLoop().schedule(new Runnable() {
				@Override
				public void run() {
					f[0] = tcpChannel.writeAndFlush(request);
					f[0].addListener(listener);
				}
			}, delayMs, TimeUnit.MILLISECONDS);

			return f[0];
		}
	}

	/**
	 * Sends a task event backwards to an intermediate result partition producer.
	 * <p>
	 * Backwards task events flow between readers and writers and therefore
	 * will only work when both are running at the same time, which is only
	 * guaranteed to be the case when both the respective producer and
	 * consumer task run pipelined.
	 */
	public void sendTaskEvent(ResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel) throws IOException {
		checkNotClosed();

		tcpChannel.writeAndFlush(new TaskEventRequest(event, partitionId, inputChannel.getInputChannelId()))
				.addListener(
						new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (!future.isSuccess()) {
									inputChannel.onError(new LocalTransportException(
											"Sending the task event failed.",
											future.channel().localAddress(), future.cause()
									));
								}
							}
						});
	}

	public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
		// We should skip the notification if the client is already closed.
		if (!closeReferenceCounter.isDisposed()) {
			partitionRequestHandler.notifyCreditAvailable(inputChannel);
		}
	}

	public void close(RemoteInputChannel inputChannel) throws IOException {

		partitionRequestHandler.removeInputChannel(inputChannel);

		if (closeReferenceCounter.decrement()) {
			// Close the TCP connection. Send a close request msg to ensure
			// that outstanding backwards task events are not discarded.
			tcpChannel.writeAndFlush(new NettyMessage.CloseRequest())
					.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

			// Make sure to remove the client from the factory
			clientFactory.destroyPartitionRequestClient(connectionId, this);
		}
		else {
			partitionRequestHandler.cancelRequestFor(inputChannel.getInputChannelId());
		}
	}

	private void checkNotClosed() throws IOException {
		if (closeReferenceCounter.isDisposed()) {
			throw new LocalTransportException("Channel closed.", tcpChannel.localAddress());
		}
	}
}
