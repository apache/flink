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

package org.apache.flink.streaming.runtime.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.CollectorWrapper;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorChain<OUT> {
	
	private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);
	
	private final StreamOperator<?>[] allOperators;
	
	private final RecordWriterOutput<?>[] streamOutputs;
	
	private final Output<StreamRecord<OUT>> chainEntryPoint;
	

	public OperatorChain(StreamTask<OUT, ?> containingTask,
							StreamOperator<OUT> headOperator,
							AccumulatorRegistry.Reporter reporter) {
		
		final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
		final StreamConfig configuration = containingTask.getConfiguration();
		final boolean enableTimestamps = containingTask.getExecutionConfig().areTimestampsEnabled();

		// we read the chained configs, and the order of record writer registrations by output name
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigs(userCodeClassloader);
		chainedConfigs.put(configuration.getVertexID(), configuration);

		// create the final output stream writers
		// we iterate through all the out edges from this job vertex and create a stream output
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(userCodeClassloader);
		Map<StreamEdge, RecordWriterOutput<?>> streamOutputMap = new HashMap<>(outEdgesInOrder.size());
		this.streamOutputs = new RecordWriterOutput<?>[outEdgesInOrder.size()];
		
		// from here on, we need to make sure that the output writers are shut down again on failure
		boolean success = false;
		try {
			for (int i = 0; i < outEdgesInOrder.size(); i++) {
				StreamEdge outEdge = outEdgesInOrder.get(i);
				
				RecordWriterOutput<?> streamOutput = createStreamOutput(
						outEdge, chainedConfigs.get(outEdge.getSourceId()), i,
						containingTask.getEnvironment(), enableTimestamps, reporter, containingTask.getName());
	
				this.streamOutputs[i] = streamOutput;
				streamOutputMap.put(outEdge, streamOutput);
			}
	
			// we create the chain of operators and grab the collector that leads into the chain
			List<StreamOperator<?>> allOps = new ArrayList<>(chainedConfigs.size());
			this.chainEntryPoint = createOutputCollector(containingTask, configuration,
					chainedConfigs, userCodeClassloader, streamOutputMap, allOps);
			
			this.allOperators = allOps.toArray(new StreamOperator<?>[allOps.size() + 1]);
			
			// add the head operator to the end of the list
			this.allOperators[this.allOperators.length - 1] = headOperator;
			
			success = true;
		}
		finally {
			// make sure we clean up after ourselves in case of a failure after acquiring
			// the first resources
			if (!success) {
				for (RecordWriterOutput<?> output : this.streamOutputs) {
					if (output != null) {
						output.close();
						output.clearBuffers();
					}
				}
			}
		}
		
	}
	
	
	public void broadcastCheckpointBarrier(long id, long timestamp) throws IOException, InterruptedException {
		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp);
		for (RecordWriterOutput<?> streamOutput : streamOutputs) {
			streamOutput.broadcastEvent(barrier);
		}
	}
	
	public RecordWriterOutput<?>[] getStreamOutputs() {
		return streamOutputs;
	}
	
	public StreamOperator<?>[] getAllOperators() {
		return allOperators;
	}

	public Output<StreamRecord<OUT>> getChainEntryPoint() {
		return chainEntryPoint;
	}

	/**
	 *
	 * This method should be called before finishing the record emission, to make sure any data
	 * that is still buffered will be sent. It also ensures that all data sending related
	 * exceptions are recognized.
	 *
	 * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
	 */
	public void flushOutputs() throws IOException {
		for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
			streamOutput.flush();
		}
	}

	/**
	 * This method releases all resources of the record writer output. It stops the output
	 * flushing thread (if there is one) and releases all buffers currently held by the output
	 * serializers.
	 *
	 * <p>This method should never fail.
	 */
	public void releaseOutputs() {
		try {
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.close();
			}
		}
		finally {
			// make sure that we release the buffers in any case
			for (RecordWriterOutput<?> output : streamOutputs) {
				output.clearBuffers();
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  initialization utilities
	// ------------------------------------------------------------------------
	
	private static <T> Output<StreamRecord<T>> createOutputCollector(
			StreamTask<?, ?> containingTask,
			StreamConfig operatorConfig,
			Map<Integer, StreamConfig> chainedConfigs,
			ClassLoader userCodeClassloader,
			Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
			List<StreamOperator<?>> allOperators)
	{
		// We create a wrapper that will encapsulate the chained operators and network outputs
		OutputSelectorWrapper<T> outputSelectorWrapper = operatorConfig.getOutputSelectorWrapper(userCodeClassloader);
		CollectorWrapper<T> wrapper = new CollectorWrapper<T>(outputSelectorWrapper);

		// create collectors for the network outputs
		for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
			@SuppressWarnings("unchecked")
			RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);
			wrapper.addCollector(output, outputEdge);
		}

		// Create collectors for the chained outputs
		for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
			int outputId = outputEdge.getTargetId();
			StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

			Output<StreamRecord<T>> output = createChainedOperator(
					containingTask, chainedOpConfig, chainedConfigs, userCodeClassloader, streamOutputs, allOperators);
			wrapper.addCollector(output, outputEdge);
		}
		return wrapper;
	}
	
	private static <IN, OUT> Output<StreamRecord<IN>> createChainedOperator(
			StreamTask<?, ?> containingTask,
			StreamConfig operatorConfig,
			Map<Integer, StreamConfig> chainedConfigs,
			ClassLoader userCodeClassloader,
			Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
			List<StreamOperator<?>> allOperators)
	{
		// create the output that the operator writes to first. this may recursively create more operators
		Output<StreamRecord<OUT>> output = createOutputCollector(
				containingTask, operatorConfig, chainedConfigs, userCodeClassloader, streamOutputs, allOperators);

		// now create the operator and give it the output collector to write its output to
		OneInputStreamOperator<IN, OUT> chainedOperator = operatorConfig.getStreamOperator(userCodeClassloader);
		chainedOperator.setup(containingTask, operatorConfig, output);

		allOperators.add(chainedOperator);

		if (containingTask.getExecutionConfig().isObjectReuseEnabled() || chainedOperator.isInputCopyingDisabled()) {
			return new ChainingOutput<IN>(chainedOperator);
		}
		else {
			TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
			return new CopyingChainingOutput<IN>(chainedOperator, inSerializer);
		}
	}
	
	private static <T> RecordWriterOutput<T> createStreamOutput(
			StreamEdge edge, StreamConfig upStreamConfig, int outputIndex,
			Environment taskEnvironment, boolean withTimestamps,
			AccumulatorRegistry.Reporter reporter, String taskName)
	{
		TypeSerializer<T> outSerializer = upStreamConfig.getTypeSerializerOut(taskEnvironment.getUserClassLoader());

		@SuppressWarnings("unchecked")
		StreamPartitioner<T> outputPartitioner = (StreamPartitioner<T>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task ", outputPartitioner, outputIndex, taskName);
		
		ResultPartitionWriter bufferWriter = taskEnvironment.getWriter(outputIndex);

		StreamRecordWriter<SerializationDelegate<StreamRecord<T>>> output = 
				new StreamRecordWriter<>(bufferWriter, outputPartitioner, upStreamConfig.getBufferTimeout());
		output.setReporter(reporter);
		
		return new RecordWriterOutput<T>(output, outSerializer, withTimestamps);
	}
	
	// ------------------------------------------------------------------------
	//  Collectors for output chaining
	// ------------------------------------------------------------------------ 

	private static class ChainingOutput<T> implements Output<StreamRecord<T>> {
		
		protected final OneInputStreamOperator<T, ?> operator;

		public ChainingOutput(OneInputStreamOperator<T, ?> operator) {
			this.operator = operator;
		}

		@Override
		public void collect(StreamRecord<T> record) {
			try {
				operator.setKeyContextElement1(record);
				operator.processElement(record);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			try {
				operator.processWatermark(mark);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void close() {
			try {
				operator.close();
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}
	}

	private static class CopyingChainingOutput<T> extends ChainingOutput<T> {
		
		private final TypeSerializer<T> serializer;
		
		public CopyingChainingOutput(OneInputStreamOperator<T, ?> operator, TypeSerializer<T> serializer) {
			super(operator);
			this.serializer = serializer;
		}

		@Override
		public void collect(StreamRecord<T> record) {
			try {

				StreamRecord<T> copy = new StreamRecord<>(serializer.copy(record.getValue()), record.getTimestamp());

				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			}
			catch (Exception e) {
				throw new RuntimeException("Could not forward element to next operator", e);
			}
		}
	}
}
