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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
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
import org.apache.flink.streaming.runtime.io.RecordWriterFactory;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputHandler<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

	private StreamTask<OUT, ?> vertex;
	private StreamConfig configuration;
	private ClassLoader cl;
	private Output<StreamRecord<OUT>> outerOutput;

	public List<StreamOperator<?>> chainedOperators;

	private Map<StreamEdge, RecordWriterOutput<?>> outputMap;

	private Map<Integer, StreamConfig> chainedConfigs;
	private List<StreamEdge> outEdgesInOrder;

	/**
	 * Counters for the number of records emitted and bytes written.
	 */
	protected AccumulatorRegistry.Reporter reporter;


	public OutputHandler(StreamTask<OUT, ?> vertex, Map<String, Accumulator<?,?>> accumulatorMap,
						AccumulatorRegistry.Reporter reporter) {

		// Initialize some fields
		this.vertex = vertex;
		this.configuration = new StreamConfig(vertex.getTaskConfiguration());
		this.chainedOperators = new ArrayList<StreamOperator<?>>();
		this.outputMap = new HashMap<StreamEdge, RecordWriterOutput<?>>();
		this.cl = vertex.getUserCodeClassLoader();

		// We read the chained configs, and the order of record writer
		// registrations by outputname
		this.chainedConfigs = configuration.getTransitiveChainedTaskConfigs(cl);
		this.chainedConfigs.put(configuration.getVertexID(), configuration);

		this.outEdgesInOrder = configuration.getOutEdgesInOrder(cl);

		this.reporter = reporter;

		// We iterate through all the out edges from this job vertex and create
		// a stream output
		for (StreamEdge outEdge : outEdgesInOrder) {
			RecordWriterOutput<?> streamOutput = createStreamOutput(
					outEdge,
					outEdge.getTargetId(),
					chainedConfigs.get(outEdge.getSourceId()),
					outEdgesInOrder.indexOf(outEdge),
					reporter);
			outputMap.put(outEdge, streamOutput);
		}

		// We create the outer output that will be passed to the first task
		// in the chain
		this.outerOutput = createChainedCollector(configuration, accumulatorMap);
		
		// Add the head operator to the end of the list
		this.chainedOperators.add(vertex.streamOperator);
	}

	public void broadcastBarrier(long id, long timestamp) throws IOException, InterruptedException {
		CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp);
		for (RecordWriterOutput<?> streamOutput : outputMap.values()) {
			streamOutput.broadcastEvent(barrier);
		}
	}

	public Collection<RecordWriterOutput<?>> getOutputs() {
		return outputMap.values();
	}
	
	public List<StreamOperator<?>> getChainedOperators(){
		return chainedOperators;
	}

	/**
	 * This method builds up a nested output which encapsulates all the
	 * chained operators and their network output. The result of this recursive
	 * call will be passed as output to the first operator in the chain.
	 *
	 * @param chainedTaskConfig
	 * 		The configuration of the starting operator of the chain, we
	 * 		use this paramater to recursively build the whole chain
	 * @return Returns the output for the chain starting from the given
	 * config
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private <X> Output<StreamRecord<X>> createChainedCollector(StreamConfig chainedTaskConfig, Map<String, Accumulator<?,?>> accumulatorMap) {


		// We create a wrapper that will encapsulate the chained operators and
		// network outputs

		OutputSelectorWrapper<?> outputSelectorWrapper = chainedTaskConfig.getOutputSelectorWrapper(cl);
		CollectorWrapper wrapper = new CollectorWrapper(outputSelectorWrapper);

		// Create collectors for the network outputs
		for (StreamEdge outputEdge : chainedTaskConfig.getNonChainedOutputs(cl)) {
			Collector<?> outCollector = outputMap.get(outputEdge);

			wrapper.addCollector(outCollector, outputEdge);
		}

		// Create collectors for the chained outputs
		for (StreamEdge outputEdge : chainedTaskConfig.getChainedOutputs(cl)) {
			Integer output = outputEdge.getTargetId();

			Collector<?> outCollector = createChainedCollector(chainedConfigs.get(output), accumulatorMap);

			wrapper.addCollector(outCollector, outputEdge);
		}

		if (chainedTaskConfig.isChainStart()) {
			// The current task is the first chained task at this vertex so we
			// return the wrapper
			return (Output<StreamRecord<X>>) wrapper;
		} else {
			// The current task is a part of the chain so we get the chainable
			// operator which will be returned and set it up using the wrapper
			OneInputStreamOperator chainableOperator =
					chainedTaskConfig.getStreamOperator(vertex.getUserCodeClassLoader());

			StreamingRuntimeContext chainedContext = vertex.createRuntimeContext(chainedTaskConfig, accumulatorMap);
			vertex.contexts.add(chainedContext);
			
			chainableOperator.setup(wrapper, chainedContext);

			chainedOperators.add(chainableOperator);
			if (vertex.getExecutionConfig().isObjectReuseEnabled() || chainableOperator.isInputCopyingDisabled()) {
				return new ChainingOutput<X>(chainableOperator);
			} else {
				StreamRecordSerializer serializerIn1;
				if (vertex.getExecutionConfig().areTimestampsEnabled()) {
					serializerIn1 = new MultiplexingStreamRecordSerializer(chainedTaskConfig.getTypeSerializerIn1(vertex.getUserCodeClassLoader()));
				} else {
					serializerIn1 = new StreamRecordSerializer(chainedTaskConfig.getTypeSerializerIn1(vertex.getUserCodeClassLoader()));
				}
				return new CopyingChainingOutput<X>(chainableOperator, (TypeSerializer<StreamRecord<X>>) serializerIn1);
			}
		}

	}

	public Output<StreamRecord<OUT>> getOutput() {
		return outerOutput;
	}

	/**
	 * We create the StreamOutput for the specific output given by the id, and
	 * the configuration of its source task
	 *
	 * @param outputVertex
	 * 		Name of the output to which the streamoutput will be set up
	 * @param upStreamConfig
	 * 		The config of upStream task
	 * @return The created StreamOutput
	 */
	private <T> RecordWriterOutput<T> createStreamOutput(StreamEdge edge, Integer outputVertex,
			StreamConfig upStreamConfig, int outputIndex, AccumulatorRegistry.Reporter reporter) {

		TypeSerializer<T> outSerializer = upStreamConfig.getTypeSerializerOut1(vertex.userClassLoader);


		@SuppressWarnings("unchecked")
		StreamPartitioner<T> outputPartitioner = (StreamPartitioner<T>) edge.getPartitioner();

		ResultPartitionWriter bufferWriter = vertex.getEnvironment().getWriter(outputIndex);

		RecordWriter<SerializationDelegate<StreamRecord<T>>> output =
				RecordWriterFactory.createRecordWriter(bufferWriter, outputPartitioner, upStreamConfig.getBufferTimeout());

		output.setReporter(reporter);

		@SuppressWarnings("unchecked")
		RecordWriterOutput<T> streamOutput = new RecordWriterOutput<T>((RecordWriter) output, outSerializer, vertex.getExecutionConfig().areTimestampsEnabled());

		if (LOG.isTraceEnabled()) {
			LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner.getClass()
					.getSimpleName(), outputIndex, vertex.getClass().getSimpleName());
		}

		return streamOutput;
	}

	public void flushOutputs() throws IOException, InterruptedException {
		for (RecordWriterOutput<?> streamOutput : getOutputs()) {
			streamOutput.close();
		}
	}

	public void clearWriters() {
		for (RecordWriterOutput<?> output : outputMap.values()) {
			output.clearBuffers();
		}
	}

	private static class ChainingOutput<T> implements Output<StreamRecord<T>> {
		protected OneInputStreamOperator operator;

		public ChainingOutput(OneInputStreamOperator<?, T> operator) {
			this.operator = operator;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void collect(StreamRecord<T> record) {
			try {
				operator.getRuntimeContext().setNextInput(record);
				operator.processElement(record);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Could not forward element to operator.", e);
				}
				throw new RuntimeException(e);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			try {
				operator.processWatermark(mark);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Could not forward element to operator: {}", e);
				}
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() {
			try {
				operator.close();
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Could not forward close call to operator.", e);
				}
			}
		}
	}

	private static class CopyingChainingOutput<T> extends ChainingOutput<T> {
		private final TypeSerializer<StreamRecord<T>> serializer;

		public CopyingChainingOutput(OneInputStreamOperator<?, T> operator,
				TypeSerializer<StreamRecord<T>> serializer) {
			super(operator);
			this.serializer = serializer;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void collect(StreamRecord<T> record) {
			try {
				operator.getRuntimeContext().setNextInput(record);
				operator.processElement(serializer.copy(record));
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Could not forward element to operator.", e);
				}
				throw new RuntimeException(e);
			}
		}
	}
}
