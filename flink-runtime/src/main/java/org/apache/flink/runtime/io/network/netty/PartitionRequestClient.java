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
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.AtomicDisposableReferenceCounter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * Partition request client for remote partition requests.
 * <p>
 * This client is shared by all remote input channels, which request a partition
 * from the same {@link RemoteAddress}.
 */
public class PartitionRequestClient {

	private final Channel tcpChannel;

	private final PartitionRequestClientHandler partitionRequestHandler;

	private final RemoteAddress remoteAddress;

	private final PartitionRequestClientFactory clientFactory;

	// If zero, the underlying TCP channel can be safely closed
	private final AtomicDisposableReferenceCounter closeReferenceCounter = new AtomicDisposableReferenceCounter();

	PartitionRequestClient(Channel tcpChannel, PartitionRequestClientHandler partitionRequestHandler, RemoteAddress remoteAddress, PartitionRequestClientFactory clientFactory) {
		this.tcpChannel = checkNotNull(tcpChannel);
		this.partitionRequestHandler = checkNotNull(partitionRequestHandler);
		this.remoteAddress = checkNotNull(remoteAddress);
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
		return closeReferenceCounter.incrementReferenceCounter();
	}

	/**
	 * Requests a remote intermediate result partition queue.
	 * <p>
	 * The request goes to the remote producer, for which this partition
	 * request client instance has been created.
	 */
	public void requestIntermediateResultPartition(ExecutionAttemptID producerExecutionId, final IntermediateResultPartitionID partitionId, final int requestedQueueIndex, final RemoteInputChannel inputChannel) throws IOException {
		partitionRequestHandler.addInputChannel(inputChannel);

		tcpChannel.writeAndFlush(new PartitionRequest(producerExecutionId, partitionId, requestedQueueIndex, inputChannel.getInputChannelId()))
				.addListener(
						new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (!future.isSuccess()) {
									partitionRequestHandler.removeInputChannel(inputChannel);
									inputChannel.onError(future.cause());
								}
							}
						}
				);
	}

	/**
	 * Sends a task event backwards to an intermediate result partition producer.
	 * <p>
	 * Backwards task events flow between readers and writers and therefore
	 * will only work when both are running at the same time, which is only
	 * guaranteed to be the case when both the respective producer and
	 * consumer task run pipelined.
	 */
	public void sendTaskEvent(ExecutionAttemptID producerExecutionId, IntermediateResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel) throws IOException {

		tcpChannel.writeAndFlush(new TaskEventRequest(event, producerExecutionId, partitionId, inputChannel.getInputChannelId()))
				.addListener(
						new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (!future.isSuccess()) {
									inputChannel.onError(future.cause());
								}
							}
						});
	}

	public void close(RemoteInputChannel inputChannel) throws IOException {

		partitionRequestHandler.removeInputChannel(inputChannel);

		if (closeReferenceCounter.decrementReferenceCounter()) {
			// Close the TCP connection
			tcpChannel.close();

			// Make sure to remove the client from the factory
			clientFactory.destroyPartitionRequestClient(remoteAddress, this);
		}
	}
}
