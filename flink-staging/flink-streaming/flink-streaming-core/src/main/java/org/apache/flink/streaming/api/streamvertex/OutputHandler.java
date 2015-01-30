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

package org.apache.flink.streaming.api.streamvertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.collector.CollectorWrapper;
import org.apache.flink.streaming.api.collector.DirectedCollectorWrapper;
import org.apache.flink.streaming.api.collector.StreamOutput;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputHandler<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

	private StreamVertex<?, OUT> vertex;
	private StreamConfig configuration;
	private ClassLoader cl;
	private Collector<OUT> outerCollector;

	public List<ChainableInvokable<?, ?>> chainedInvokables;

	private Map<String, StreamOutput<?>> outputMap;
	private Map<String, StreamConfig> chainedConfigs;
	private List<Tuple2<String, String>> outEdgesInOrder;

	public OutputHandler(StreamVertex<?, OUT> vertex) {

		// Initialize some fields
		this.vertex = vertex;
		this.configuration = new StreamConfig(vertex.getTaskConfiguration());
		this.chainedInvokables = new ArrayList<ChainableInvokable<?, ?>>();
		this.outputMap = new HashMap<String, StreamOutput<?>>();
		this.cl = vertex.getUserCodeClassLoader();

		// We read the chained configs, and the order of record writer
		// registrations by outputname
		this.chainedConfigs = configuration.getTransitiveChainedTaskConfigs(cl);
		this.chainedConfigs.put(configuration.getTaskName(), configuration);

		this.outEdgesInOrder = configuration.getOutEdgesInOrder(cl);

		// We iterate through all the out edges from this job vertex and create
		// a stream output
		for (Tuple2<String, String> outEdge : outEdgesInOrder) {
			StreamOutput<?> streamOutput = createStreamOutput(outEdge.f1,
					chainedConfigs.get(outEdge.f0), outEdgesInOrder.indexOf(outEdge));
			outputMap.put(outEdge.f1, streamOutput);
		}

		// We create the outer collector that will be passed to the first task
		// in the chain
		this.outerCollector = createChainedCollector(configuration);

	}

	public Collection<StreamOutput<?>> getOutputs() {
		return outputMap.values();
	}

	/**
	 * This method builds up a nested collector which encapsulates all the
	 * chained operators and their network output. The result of this recursive
	 * call will be passed as collector to the first invokable in the chain.
	 * 
	 * @param chainedTaskConfig
	 *            The configuration of the starting operator of the chain, we
	 *            use this paramater to recursively build the whole chain
	 * @return Returns the collector for the chain starting from the given
	 *         config
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Collector<OUT> createChainedCollector(StreamConfig chainedTaskConfig) {

		boolean isDirectEmit = chainedTaskConfig.isDirectedEmit();

		// We create a wrapper that will encapsulate the chained operators and
		// network outputs
		CollectorWrapper<OUT> wrapper = isDirectEmit ? new DirectedCollectorWrapper(
				chainedTaskConfig.getOutputSelectors(cl)) : new CollectorWrapper<OUT>();

		// Create collectors for the network outputs
		for (String output : chainedTaskConfig.getOutputs(cl)) {

			Collector<?> outCollector = outputMap.get(output);

			if (isDirectEmit) {
				((DirectedCollectorWrapper<OUT>) wrapper).addCollector(outCollector,
						chainedTaskConfig.getSelectedNames(output));
			} else {
				wrapper.addCollector(outCollector);
			}
		}

		// Create collectors for the chained outputs
		for (String output : chainedTaskConfig.getChainedOutputs(cl)) {
			Collector<?> outCollector = createChainedCollector(chainedConfigs.get(output));
			if (isDirectEmit) {
				((DirectedCollectorWrapper<OUT>) wrapper).addCollector(outCollector,
						chainedTaskConfig.getSelectedNames(output));
			} else {
				wrapper.addCollector(outCollector);
			}
		}

		if (chainedTaskConfig.isChainStart()) {
			// The current task is the first chained task at this vertex so we
			// return the wrapper
			return wrapper;
		} else {
			// The current task is a part of the chain so we get the chainable
			// invokable which will be returned and set it up using the wrapper
			ChainableInvokable chainableInvokable = chainedTaskConfig.getUserInvokable(vertex
					.getUserCodeClassLoader());
			chainableInvokable.setup(wrapper,
					chainedTaskConfig.getTypeSerializerIn1(vertex.getUserCodeClassLoader()));

			chainedInvokables.add(chainableInvokable);
			return chainableInvokable;
		}

	}

	public Collector<OUT> getCollector() {
		return outerCollector;
	}

	/**
	 * We create the StreamOutput for the specific output given by the name, and
	 * the configuration of its source task
	 * 
	 * @param outputVertex
	 *            Name of the output to which the streamoutput will be set up
	 * @param configuration
	 *            The config of upStream task
	 * @return
	 */
	private <T> StreamOutput<T> createStreamOutput(String outputVertex, StreamConfig configuration,
			int outputIndex) {

		StreamRecordSerializer<T> outSerializer = configuration
				.getTypeSerializerOut1(vertex.userClassLoader);
		SerializationDelegate<StreamRecord<T>> outSerializationDelegate = null;

		if (outSerializer != null) {
			outSerializationDelegate = new SerializationDelegate<StreamRecord<T>>(outSerializer);
			outSerializationDelegate.setInstance(outSerializer.createInstance());
		}

		StreamPartitioner<T> outputPartitioner = configuration.getPartitioner(cl, outputVertex);

		RecordWriter<SerializationDelegate<StreamRecord<T>>> output;

		if (configuration.getBufferTimeout() >= 0) {

			output = new StreamRecordWriter<SerializationDelegate<StreamRecord<T>>>(vertex
					.getEnvironment().getWriter(outputIndex), outputPartitioner,
					configuration.getBufferTimeout());

			if (LOG.isTraceEnabled()) {
				LOG.trace("StreamRecordWriter initiated with {} bufferTimeout for {}",
						configuration.getBufferTimeout(), vertex.getClass().getSimpleName());
			}
		} else {
			output = new RecordWriter<SerializationDelegate<StreamRecord<T>>>(vertex
					.getEnvironment().getWriter(outputIndex), outputPartitioner);

			if (LOG.isTraceEnabled()) {
				LOG.trace("RecordWriter initiated for {}", vertex.getClass().getSimpleName());
			}
		}

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

	public void invokeUserFunction(String componentTypeName, StreamInvokable<?, OUT> userInvokable)
			throws IOException, InterruptedException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoked with instance id {}", componentTypeName, vertex.getName(),
					vertex.getInstanceID());
		}

		try {
			vertex.invokeUserFunction(userInvokable);
		} catch (Exception e) {
			flushOutputs();
			throw new RuntimeException(e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} {} invoke finished instance id {}", componentTypeName, vertex.getName(),
					vertex.getInstanceID());
		}

		flushOutputs();
	}
}
