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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A record-oriented runtime result writer API for producing results.
 *
 * <p>If {@link ResultPartitionWriter#close()} is called before {@link ResultPartitionWriter#release(Throwable)}}
 * or {@link ResultPartitionWriter#finish()}, it abruptly triggers failure and cancellation of production. In this
 * case {@link ResultPartitionWriter#release(Throwable)}} still needs to be called afterwards to fully release all
 * resources associated the the partition and propagate failure cause to the consumer if possible.
 */
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {

	/**
	 * Setup partition, potentially heavy-weight, blocking operation comparing to just creation.
	 */
	void setup() throws IOException;

	/**
	 * Reads the previous output states with the given reader for unaligned checkpoint.
	 * It should be done before task processing the inputs.
	 */
	void readRecoveredState(ChannelStateReader stateReader) throws IOException, InterruptedException;

	ResultPartitionID getPartitionId();

	int getNumberOfSubpartitions();

	int getNumTargetKeyGroups();

	/**
	 * This is used to send regular records to the given channel.
	 */
	void writerRecord(ByteBuffer record, int targetChannel, boolean isBroadcastSelector) throws IOException, InterruptedException;

	/**
	 * This is used to broadcast regular records or streaming Watermarks in-band with records to all channels.
	 */
	void broadcastWrite(ByteBuffer record, boolean isBroadcastSelector) throws IOException, InterruptedException;

	/**
	 * This is used to broadcast event to all channels.
	 */
	void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

	void setMetricGroup(TaskIOMetricGroup metrics);

	void setConsumableNotifier(Runnable consumableNotifier);

	/**
	 * Returns the subpartition with the given index.
	 */
	ResultSubpartition getSubpartition(int subpartitionIndex);

	/**
	 * Manually trigger consumption of all subpartitions.
	 */
	void flushAll();

	/**
	 * Manually trigger consumption of the specified subpartition.
	 */
	void flush(int subpartitionIndex);

	/**
	 * Returns the read view of the requested subpartition.
	 */
	ResultSubpartitionView createSubpartitionView(int subpartitionIndex, BufferAvailabilityListener listener) throws IOException;

	/**
	 * Successfully finish the production of the partition.
	 *
	 * <p>Closing of partition is still needed afterwards.
	 */
	void finish() throws IOException;

	/**
	 * Releases the result partition writer.
	 */
	void release(@Nullable Throwable cause);

	boolean isFinished();

	boolean isReleased();
}
