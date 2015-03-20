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

import org.apache.commons.lang.StringUtils;
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

	private Map<Integer, AbstractJobVertex> streamVertices;
	private JobGraph jobGraph;
	private Collection<Integer> builtVertices;

	private List<StreamEdge> physicalEdgesInOrder;

	private Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private Map<Integer, StreamConfig> vertexConfigs;
	private Map<Integer, String> chainedNames;

	public StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
	}

	private void init() {
		this.streamVertices = new HashMap<Integer, AbstractJobVertex>();
		this.builtVertices = new HashSet<Integer>();
		this.chainedConfigs = new HashMap<Integer, Map<Integer, StreamConfig>>();
		this.vertexConfigs = new HashMap<Integer, StreamConfig>();
		this.chainedNames = new HashMap<Integer, String>();
		this.physicalEdgesInOrder = new ArrayList<StreamEdge>();
	}

	public JobGraph createJobGraph(String jobName) {
		jobGraph = new JobGraph(jobName);

		// Turn lazy scheduling off
		jobGraph.setScheduleMode(ScheduleMode.ALL);
		jobGraph.setJobType(JobGraph.JobType.STREAMING);
		jobGraph.setMonitoringEnabled(streamGraph.isMonitoringEnabled());
		jobGraph.setMonitorInterval(streamGraph.getMonitoringInterval());
		if (jobGraph.isMonitoringEnabled()) {
			jobGraph.setNumberOfExecutionRetries(Integer.MAX_VALUE);
		}
		init();

		setChaining();

		setPhysicalEdges();

		setSlotSharing();

		return jobGraph;
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetVertex();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.get(target);

			// create if not set
			if (inEdges == null) {
				inEdges = new ArrayList<StreamEdge>();
				physicalInEdgesInOrder.put(target, inEdges);
			}

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	private void setChaining() {
		for (Integer sourceName : streamGraph.getSources()) {
			createChain(sourceName, sourceName);
		}
	}

	private List<StreamEdge> createChain(Integer startNode, Integer current) {

		if (!builtVertices.contains(startNode)) {

			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			for (StreamEdge outEdge : streamGraph.getOutEdges(current)) {
				if (isChainable(outEdge)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(createChain(startNode, chainable.getTargetVertex()));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetVertex(), nonChainable.getTargetVertex());
			}

			chainedNames.put(current, createChainedName(current, chainableOutputs));

			StreamConfig config = current.equals(startNode) ? createProcessingVertex(startNode)
					: new StreamConfig(new Configuration());

			setVertexConfig(current, config, chainableOutputs, nonChainableOutputs);

			if (current.equals(startNode)) {

				config.setChainStart();
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getOutEdges(current));

				for (StreamEdge edge : transitiveOutEdges) {
					connect(startNode, edge);
				}

				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNode));

			} else {

				Map<Integer, StreamConfig> chainedConfs = chainedConfigs.get(startNode);

				if (chainedConfs == null) {
					chainedConfigs.put(startNode, new HashMap<Integer, StreamConfig>());
				}
				chainedConfigs.get(startNode).put(current, config);
			}

			return transitiveOutEdges;

		} else {
			return new ArrayList<StreamEdge>();
		}
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getOperatorName(vertexID);
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<String>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetVertex()));
			}
			String returnOperatorName = operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
			return returnOperatorName;
		} else if (chainedOutputs.size() == 1) {
			String returnOperatorName = operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetVertex());
			return returnOperatorName;
		} else {
			return operatorName;
		}

	}

	private StreamConfig createProcessingVertex(Integer vertexID) {

		AbstractJobVertex vertex = new AbstractJobVertex(chainedNames.get(vertexID));

		vertex.setInvokableClass(streamGraph.getJobVertexClass(vertexID));
		if (streamGraph.getParallelism(vertexID) > 0) {
			vertex.setParallelism(streamGraph.getParallelism(vertexID));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", streamGraph.getParallelism(vertexID),
					vertexID);
		}

		if (streamGraph.getInputFormat(vertexID) != null) {
			vertex.setInputSplitSource(streamGraph.getInputFormat(vertexID));
		}

		streamVertices.put(vertexID, vertex);
		builtVertices.add(vertexID);
		jobGraph.addVertex(vertex);

		StreamConfig retConfig = new StreamConfig(vertex.getConfiguration());
		retConfig.setOperatorName(chainedNames.get(vertexID));
		return retConfig;
	}

	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		config.setVertexID(vertexID);
		config.setBufferTimeout(streamGraph.getBufferTimeout(vertexID));

		config.setTypeSerializerIn1(streamGraph.getInSerializer1(vertexID));
		config.setTypeSerializerIn2(streamGraph.getInSerializer2(vertexID));
		config.setTypeSerializerOut1(streamGraph.getOutSerializer1(vertexID));
		config.setTypeSerializerOut2(streamGraph.getOutSerializer2(vertexID));

		config.setUserInvokable(streamGraph.getInvokable(vertexID));
		config.setOutputSelectorWrapper(streamGraph.getOutputSelectorWrapper(vertexID));

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);
		config.setStateMonitoring(streamGraph.isMonitoringEnabled());

		Class<? extends AbstractInvokable> vertexClass = streamGraph.getJobVertexClass(vertexID);

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getIterationID(vertexID));
			config.setIterationWaitTime(streamGraph.getIterationTimeout(vertexID));
		}

		List<StreamEdge> allOutputs = new ArrayList<StreamEdge>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		for (StreamEdge output : allOutputs) {
			config.setSelectedNames(output.getTargetVertex(), streamGraph.getEdge(vertexID, output.getTargetVertex()).getSelectedNames());
		}

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetVertex();

		AbstractJobVertex headVertex = streamVertices.get(headOfChain);
		AbstractJobVertex downStreamVertex = streamVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();
		if (partitioner.getStrategy() == PartitioningStrategy.FORWARD) {
			downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.POINTWISE);
		} else {
			downStreamVertex.connectNewDataSetAsInput(headVertex, DistributionPattern.ALL_TO_ALL);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	private boolean isChainable(StreamEdge edge) {
		int vertexID = edge.getSourceVertex();
		int outName = edge.getTargetVertex();

		StreamInvokable<?, ?> headInvokable = streamGraph.getInvokable(vertexID);
		StreamInvokable<?, ?> outInvokable = streamGraph.getInvokable(outName);

		return
				streamGraph.getInEdges(outName).size() == 1
						&& outInvokable != null
						&& outInvokable.getChainingStrategy() == ChainingStrategy.ALWAYS
						&& (headInvokable.getChainingStrategy() == ChainingStrategy.HEAD || headInvokable
						.getChainingStrategy() == ChainingStrategy.ALWAYS)
						&& edge.getPartitioner().getStrategy() == PartitioningStrategy.FORWARD
						&& streamGraph.getParallelism(vertexID) == streamGraph.getParallelism(outName)
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
