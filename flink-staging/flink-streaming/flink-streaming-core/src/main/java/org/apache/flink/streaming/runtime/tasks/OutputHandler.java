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

import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.CollectorWrapper;
import org.apache.flink.streaming.api.collector.StreamOutput;
import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.ChainableStreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterFactory;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputHandler<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

	private StreamTask<?, OUT> vertex;
	private StreamConfig configuration;
	private ClassLoader cl;
	private Collector<OUT> outerCollector;

	private List<ChainableStreamOperator<?, ?>> chainedOperators;

	private Map<StreamEdge, StreamOutput<?>> outputMap;

	private Map<Integer, StreamConfig> chainedConfigs;
	private List<StreamEdge> outEdgesInOrder;

	public OutputHandler(StreamTask<?, OUT> vertex) {

		// Initialize some fields
		this.vertex = vertex;
		this.configuration = new StreamConfig(vertex.getTaskConfiguration());
		this.chainedOperators = new ArrayList<ChainableStreamOperator<?, ?>>();
		this.outputMap = new HashMap<StreamEdge, StreamOutput<?>>();
		this.cl = vertex.getUserCodeClassLoader();

		// We read the chained configs, and the order of record writer
		// registrations by outputname
		this.chainedConfigs = configuration.getTransitiveChainedTaskConfigs(cl);
		this.chainedConfigs.put(configuration.getVertexID(), configuration);

		this.outEdgesInOrder = configuration.getOutEdgesInOrder(cl);

		// We iterate through all the out edges from this job vertex and create
		// a stream output
		for (StreamEdge outEdge : outEdgesInOrder) {
			StreamOutput<?> streamOutput = createStreamOutput(
					outEdge,
					outEdge.getTargetID(),
					chainedConfigs.get(outEdge.getSourceID()),
					outEdgesInOrder.indexOf(outEdge));
			outputMap.put(outEdge, streamOutput);
		}

		// We create the outer collector that will be passed to the first task
		// in the chain
		this.outerCollector = createChainedCollector(configuration);
	}

	public void broadcastBarrier(long id, long timestamp) throws IOException, InterruptedException {
		StreamingSuperstep barrier = new StreamingSuperstep(id, timestamp);
		for (StreamOutput<?> streamOutput : outputMap.values()) {
			streamOutput.broadcastEvent(barrier);
		}
	}

	public Collection<StreamOutput<?>> getOutputs() {
		return outputMap.values();
	}
	
	public List<ChainableStreamOperator<?, ?>> getChainedOperators(){
		return chainedOperators;
	}

	/**
	 * This method builds up a nested collector which encapsulates all the
	 * chained operators and their network output. The result of this recursive
	 * call will be passed as collector to the first operator in the chain.
	 *
	 * @param chainedTaskConfig
	 * 		The configuration of the starting operator of the chain, we
	 * 		use this paramater to recursively build the whole chain
	 * @return Returns the collector for the chain starting from the given
	 * config
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private Collector<OUT> createChainedCollector(StreamConfig chainedTaskConfig) {


		// We create a wrapper that will encapsulate the chained operators and
		// network outputs

		OutputSelectorWrapper<OUT> outputSelectorWrapper = chainedTaskConfig.getOutputSelectorWrapper(cl);
		CollectorWrapper<OUT> wrapper = new CollectorWrapper<OUT>(outputSelectorWrapper);

		// Create collectors for the network outputs
		for (StreamEdge outputEdge : chainedTaskConfig.getNonChainedOutputs(cl)) {
			Collector<?> outCollector = outputMap.get(outputEdge);

			wrapper.addCollector(outCollector, outputEdge);
		}

		// Create collectors for the chained outputs
		for (StreamEdge outputEdge : chainedTaskConfig.getChainedOutputs(cl)) {
			Integer output = outputEdge.getTargetID();

			Collector<?> outCollector = createChainedCollector(chainedConfigs.get(output));

			wrapper.addCollector(outCollector, outputEdge);
		}

		if (chainedTaskConfig.isChainStart()) {
			// The current task is the first chained task at this vertex so we
			// return the wrapper
			return wrapper;
		} else {
			// The current task is a part of the chain so we get the chainable
			// operator which will be returned and set it up using the wrapper
			ChainableStreamOperator chainableOperator = chainedTaskConfig.getStreamOperator(vertex
					.getUserCodeClassLoader());
			chainableOperator.setup(wrapper,
					chainedTaskConfig.getTypeSerializerIn1(vertex.getUserCodeClassLoader()));

			chainedOperators.add(chainableOperator);
			return chainableOperator;
		}

	}

	public Collector<OUT> getCollector() {
		return outerCollector;
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
	private <T> StreamOutput<T> createStreamOutput(StreamEdge edge, Integer outputVertex,
			StreamConfig upStreamConfig, int outputIndex) {

		StreamRecordSerializer<T> outSerializer = upStreamConfig
				.getTypeSerializerOut1(vertex.userClassLoader);
		SerializationDelegate<StreamRecord<T>> outSerializationDelegate = null;

		if (outSerializer != null) {
			outSerializationDelegate = new SerializationDelegate<StreamRecord<T>>(outSerializer);
			outSerializationDelegate.setInstance(outSerializer.createInstance());
		}

		@SuppressWarnings("unchecked")
		StreamPartitioner<T> outputPartitioner = (StreamPartitioner<T>) edge.getPartitioner();

		ResultPartitionWriter bufferWriter = vertex.getEnvironment().getWriter(outputIndex);

		RecordWriter<SerializationDelegate<StreamRecord<T>>> output =
				RecordWriterFactory.createRecordWriter(bufferWriter, outputPartitioner, upStreamConfig.getBufferTimeout());

		StreamOutput<T> streamOutput = new StreamOutput<T>(output, vertex.instanceID,
				outSerializationDelegate);

		if (LOG.isTraceEnabled()) {
			LOG.trace("Partitioner set: {} with {} outputs for {}", outputPartitioner.getClass()
					.getSimpleName(), outputIndex, vertex.getClass().getSimpleName());
		}

		return streamOutput;
	}

	public void flushOutputs() throws IOException, InterruptedException {
		for (StreamOutput<?> streamOutput : getOutputs()) {
			streamOutput.close();
		}
	}

	public void clearWriters() {
		for (StreamOutput<?> output : outputMap.values()) {
			output.clearBuffers();
		}
	}
}
