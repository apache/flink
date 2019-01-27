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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import java.io.IOException;

/**
 * A buffer-oriented runtime result writer API for producing results.
 */
public interface ResultPartitionWriter<T> {

	ResultPartitionID getPartitionId();

	int getNumberOfSubpartitions();

	int getNumTargetKeyGroups();

	/**
	 * Adds a record to all target channels.
	 *
	 * @param record         The record to write.
	 * @param targetChannels The target channels.
	 * @param isBroadcast    Whether broadcast or not.
	 * @param flushAlways  Whether flush or not.
	 */
	void emitRecord(T record, int[] targetChannels, boolean isBroadcast, boolean flushAlways) throws IOException, InterruptedException;

	/**
	 * Adds a record to a random channel.
	 *
	 * @param record         The record to write.
	 * @param targetChannel The target channel.
	 * @param flushAlways  Whether flush or not.
	 */
	void emitRecord(T record, int targetChannel, boolean isBroadcast, boolean flushAlways) throws IOException, InterruptedException;

	/**
	 * Broadcasts an event to all subpartitions.
	 *
	 * @param event The event to be broadcast.
	 * @param flushAlways  Whether to flush or not.
	 */
	void broadcastEvent(AbstractEvent event, boolean flushAlways) throws IOException;

	/**
	 * Closes all the buffer builders.
	 */
	void clearBuffers();

	/**
	 * Sets the metric group for this ResultPartitionWriter.
	 */
	void setMetricGroup(TaskIOMetricGroup metrics, boolean enableTracingMetrics, int tracingMetricsInterval);

	/**
	 * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in all subpartitions.
	 */
	void flushAll();

	/**
	 * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in one specified subpartition.
	 */
	void flush(int subpartitionIndex);

	/**
	 * Sets the serialization and deserialization serializer.
	 *
	 * @param typeSerializer The type serializer used.
	 */
	void setTypeSerializer(TypeSerializer typeSerializer);

	/**
	 * Sets the parent task.
	 *
	 * @param parentTask The parent task.
	 */
	void setParentTask(AbstractInvokable parentTask);
}
