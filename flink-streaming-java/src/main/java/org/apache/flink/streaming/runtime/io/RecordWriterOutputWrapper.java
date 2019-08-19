/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The wrapper for creating an array of {@link RecordWriterOutput}s and handling the relevant
 * logic for them.
 */
public class RecordWriterOutputWrapper {
	private static final Logger LOG = LoggerFactory.getLogger(RecordWriterOutputWrapper.class);

	private final RecordWriterOutput<?>[] recordWriterOutputs;

	private RecordWriterOutputWrapper(RecordWriterOutput<?>[] recordWriterOutputs) {
		this.recordWriterOutputs = Preconditions.checkNotNull(recordWriterOutputs);
	}

	public void broadcastCheckpointBarrier(
			long id,
			long timestamp,
			CheckpointOptions checkpointOptions) throws IOException {
		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, checkpointOptions);
		for (RecordWriterOutput<?> output : recordWriterOutputs) {
			output.broadcastEvent(barrier);
		}
	}

	public void broadcastCheckpointCancelMarker(long id) throws IOException {
		CancelCheckpointMarker barrier = new CancelCheckpointMarker(id);
		for (RecordWriterOutput<?> output : recordWriterOutputs) {
			output.broadcastEvent(barrier);
		}
	}

	/**
	 * This method should be called before finishing the record emission, to make sure any data
	 * that is still buffered will be sent. It also ensures that all data sending related
	 * exceptions are recognized.
	 *
	 * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
	 */
	public void flushOutputs() throws IOException {
		for (RecordWriterOutput<?> output : recordWriterOutputs) {
			output.flush();
		}
	}

	/**
	 * This method releases all resources of the record writer output. It stops the output
	 * flushing thread (if there is one) and releases all buffers currently held by the
	 * output serializers.
	 *
	 * <p>This method should never fail.
	 */
	public void releaseOutputs() {
		for (RecordWriterOutput<?> output : recordWriterOutputs) {
			output.close();
		}
	}

	public RecordWriterOutput<?>[] getRecordWriterOutputs() {
		return recordWriterOutputs;
	}

	private static RecordWriter<SerializationDelegate<StreamRecord<?>>> createRecordWriter(
			StreamEdge edge,
			int outputIndex,
			Environment environment,
			long bufferTimeout) {
		StreamPartitioner<?> outputPartitioner = edge.getPartitioner();
		String taskName = environment.getTaskInfo().getTaskName();

		LOG.debug("Using partitioner {} for output {} of task {}", outputPartitioner, outputIndex, taskName);

		ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		RecordWriter<SerializationDelegate<StreamRecord<?>>> output = new RecordWriterBuilder()
			.setChannelSelector(outputPartitioner)
			.setTimeout(bufferTimeout)
			.setTaskName(taskName)
			.build(bufferWriter);
		output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
		return output;
	}

	public static RecordWriterOutputWrapper build(StreamConfig config, Environment environment) {
		ClassLoader userClassLoader = environment.getUserClassLoader();
		List<StreamEdge> outEdgesInOrder = config.getOutEdgesInOrder(userClassLoader);
		Map<Integer, StreamConfig> chainedConfigs = config.getTransitiveChainedTaskConfigsWithSelf(
			userClassLoader);
		RecordWriterOutput<?>[] outputs = new RecordWriterOutput[outEdgesInOrder.size()];

		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge edge = outEdgesInOrder.get(i);
			OutputTag outputTag = edge.getOutputTag();
			StreamConfig chainedConfig = chainedConfigs.get(edge.getSourceId());
			RecordWriter<SerializationDelegate<StreamRecord<?>>> recordWriter = createRecordWriter(
				edge,
				i,
				environment,
				chainedConfig.getBufferTimeout());

			TypeSerializer outSerializer;
			if (edge.getOutputTag() != null) {
				// side output
				outSerializer = chainedConfig.getTypeSerializerSideOut(outputTag, userClassLoader);
			} else {
				// main output
				outSerializer = chainedConfig.getTypeSerializerOut(userClassLoader);
			}
			outputs[i] = new RecordWriterOutput(recordWriter, outSerializer, outputTag);
		}
		return new RecordWriterOutputWrapper(outputs);
	}
}
