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

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable.ChainingStrategy;
import org.apache.flink.streaming.api.streamvertex.StreamIterationHead;
import org.apache.flink.streaming.api.streamvertex.StreamIterationTail;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner.PartitioningStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	private StreamGraph streamGraph;

	private Map<String, AbstractJobVertex> streamVertices;
	private JobGraph jobGraph;
	private Collection<String> builtNodes;

	private Map<String, Map<String, StreamConfig>> chainedConfigs;
	private Map<String, StreamConfig> vertexConfigs;
	private Map<String, String> chainedNames;

	public StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
	}

	private void init() {
		this.streamVertices = new HashMap<String, AbstractJobVertex>();
		this.builtNodes = new HashSet<String>();
		this.chainedConfigs = new HashMap<String, Map<String, StreamConfig>>();
		this.vertexConfigs = new HashMap<String, StreamConfig>();
		this.chainedNames = new HashMap<String, String>();
	}

	public JobGraph createJobGraph(String jobName) {
		jobGraph = new JobGraph(jobName);
		// Turn lazy scheduling off
		jobGraph.setScheduleMode(ScheduleMode.ALL);

		init();

		for (String sourceName : streamGraph.getSources()) {
			createChain(sourceName, sourceName);
		}

		setSlotSharing();

		return jobGraph;
	}

	private List<Tuple2<String, String>> createChain(String startNode, String current) {

		if (!builtNodes.contains(startNode)) {

			List<Tuple2<String, String>> transitiveOutEdges = new ArrayList<Tuple2<String, String>>();
			List<String> chainableOutputs = new ArrayList<String>();
			List<String> nonChainableOutputs = new ArrayList<String>();

			for (String outName : streamGraph.getOutEdges(current)) {
				if (isChainable(current, outName)) {
					chainableOutputs.add(outName);
				} else {
					nonChainableOutputs.add(outName);
				}
			}

			for (String chainable : chainableOutputs) {
				transitiveOutEdges.addAll(createChain(startNode, chainable));
			}

			for (String nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(new Tuple2<String, String>(current, nonChainable));
				createChain(nonChainable, nonChainable);
			}

			chainedNames.put(current, createChainedName(current, chainableOutputs));

			StreamConfig config = current.equals(startNode) ? createProcessingVertex(startNode)
					: new StreamConfig(new Configuration());

			setVertexConfig(current, config, chainableOutputs, nonChainableOutputs);

			if (current.equals(startNode)) {

				config.setChainStart();
				config.setOutEdgesInOrder(transitiveOutEdges);

				for (Tuple2<String, String> edge : transitiveOutEdges) {
					connect(startNode, edge);
				}

				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNode));

			} else {

				Map<String, StreamConfig> chainedConfs = chainedConfigs.get(startNode);

				if (chainedConfs == null) {
					chainedConfigs.put(startNode, new HashMap<String, StreamConfig>());
				}
				chainedConfigs.get(startNode).put(current, config);
			}

			return transitiveOutEdges;

		} else {
			return new ArrayList<Tuple2<String, String>>();
		}
	}

	private String createChainedName(String vertexID, List<String> chainedOutputs) {
		String vertexName = streamGraph.getOperatorName(vertexID);
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<String>();
			for (String chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable));
			}
			return vertexName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return vertexName + " -> " + chainedNames.get(chainedOutputs.get(0));
		} else {
			return vertexName;
		}

	}

	private StreamConfig createProcessingVertex(String vertexName) {

		AbstractJobVertex vertex = new AbstractJobVertex(chainedNames.get(vertexName));

		vertex.setInvokableClass(streamGraph.getJobVertexClass(vertexName));
		if (streamGraph.getParallelism(vertexName) > 0) {
			vertex.setParallelism(streamGraph.getParallelism(vertexName));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", streamGraph.getParallelism(vertexName),
					vertexName);
		}

		if (streamGraph.getInputFormat(vertexName) != null) {
			vertex.setInputSplitSource(streamGraph.getInputFormat(vertexName));
		}

		streamVertices.put(vertexName, vertex);
		builtNodes.add(vertexName);
		jobGraph.addVertex(vertex);

		return new StreamConfig(vertex.getConfiguration());
	}

	private void setVertexConfig(String vertexName, StreamConfig config,
			List<String> chainableOutputs, List<String> nonChainableOutputs) {

		config.setVertexName(vertexName);
		config.setBufferTimeout(streamGraph.getBufferTimeout(vertexName));

		config.setTypeSerializerIn1(streamGraph.getInSerializer1(vertexName));
		config.setTypeSerializerIn2(streamGraph.getInSerializer2(vertexName));
		config.setTypeSerializerOut1(streamGraph.getOutSerializer1(vertexName));
		config.setTypeSerializerOut2(streamGraph.getOutSerializer2(vertexName));

		config.setUserInvokable(streamGraph.getInvokable(vertexName));
		config.setOutputSelectors(streamGraph.getOutputSelector(vertexName));
		config.setOperatorStates(streamGraph.getState(vertexName));

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		Class<? extends AbstractInvokable> vertexClass = streamGraph.getJobVertexClass(vertexName);

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getIterationID(vertexName));
			config.setIterationWaitTime(streamGraph.getIterationTimeout(vertexName));
		}

		List<String> allOutputs = new ArrayList<String>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		for (String output : allOutputs) {
			config.setSelectedNames(output, streamGraph.getSelectedNames(vertexName, output));
		}

		vertexConfigs.put(vertexName, config);
	}

	private <T> void connect(String headOfChain, Tuple2<String, String> edge) {

		String upStreamVertexName = edge.f0;
		String downStreamVertexName = edge.f1;

		int outputIndex = streamGraph.getOutEdges(upStreamVertexName).indexOf(downStreamVertexName);

		AbstractJobVertex headVertex = streamVertices.get(headOfChain);
		AbstractJobVertex downStreamVertex = streamVertices.get(downStreamVertexName);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());
		StreamConfig upStreamConfig = headOfChain == upStreamVertexName ? new StreamConfig(
				headVertex.getConfiguration()) : chainedConfigs.get(headOfChain).get(
				upStreamVertexName);

		List<Integer> outEdgeIndexList = streamGraph.getOutEdgeTypes(upStreamVertexName);
		int numOfInputs = downStreamConfig.getNumberOfInputs();

		downStreamConfig.setInputIndex(numOfInputs++, outEdgeIndexList.get(outputIndex));
		downStreamConfig.setNumberOfInputs(numOfInputs);

		StreamPartitioner<?> partitioner = streamGraph.getOutPartitioner(upStreamVertexName,
				downStreamVertexName);

		upStreamConfig.setPartitioner(downStreamVertexName, partitioner);

		if (partitioner.getStrategy() == PartitioningStrategy.FORWARD) {
			downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.POINTWISE);
		} else {
			downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.ALL_TO_ALL);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamVertexName);
		}
	}

	private boolean isChainable(String vertexName, String outName) {

		StreamInvokable<?, ?> headInvokable = streamGraph.getInvokable(vertexName);
		StreamInvokable<?, ?> outInvokable = streamGraph.getInvokable(outName);

		return streamGraph.getInEdges(outName).size() == 1
				&& outInvokable != null
				&& outInvokable.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headInvokable.getChainingStrategy() == ChainingStrategy.HEAD || headInvokable
						.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& streamGraph.getOutPartitioner(vertexName, outName).getStrategy() == PartitioningStrategy.FORWARD
				&& streamGraph.getParallelism(vertexName) == streamGraph.getParallelism(outName)
				&& streamGraph.chaining;
	}

	private void setSlotSharing() {
		SlotSharingGroup shareGroup = new SlotSharingGroup();

		for (AbstractJobVertex vertex : streamVertices.values()) {
			vertex.setSlotSharingGroup(shareGroup);
		}

		for (Integer iterID : streamGraph.getIterationIDs()) {
			CoLocationGroup ccg = new CoLocationGroup();
			AbstractJobVertex tail = streamVertices.get(streamGraph.getIterationTail(iterID));
			AbstractJobVertex head = streamVertices.get(streamGraph.getIterationHead(iterID));

			ccg.addVertex(head);
			ccg.addVertex(tail);
		}
	}
}
