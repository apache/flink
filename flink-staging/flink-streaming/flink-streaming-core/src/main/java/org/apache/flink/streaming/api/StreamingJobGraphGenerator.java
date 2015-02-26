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

	private Map<Integer, AbstractJobVertex> streamVertices;
	private JobGraph jobGraph;
	private Collection<Integer> builtVertices;

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
	}

	public JobGraph createJobGraph(String jobName) {
		jobGraph = new JobGraph(jobName);

		// Turn lazy scheduling off
		jobGraph.setScheduleMode(ScheduleMode.ALL);
		jobGraph.setJobType(JobGraph.JobType.STREAMING);
		jobGraph.setMonitoringEnabled(streamGraph.isMonitoringEnabled());
		jobGraph.setMonitorInterval(streamGraph.getMonitoringInterval());
		if(jobGraph.isMonitoringEnabled())
		{
			jobGraph.setNumberOfExecutionRetries(Integer.MAX_VALUE);
		}
		init();

		setChaining();

		setSlotSharing();

		return jobGraph;
	}

	private void setChaining() {
		for (Integer sourceName : streamGraph.getSources()) {
			createChain(sourceName, sourceName);
		}
	}

	private List<Tuple2<Integer, Integer>> createChain(Integer startNode, Integer current) {

		if (!builtVertices.contains(startNode)) {

			List<Tuple2<Integer, Integer>> transitiveOutEdges = new ArrayList<Tuple2<Integer, Integer>>();
			List<Integer> chainableOutputs = new ArrayList<Integer>();
			List<Integer> nonChainableOutputs = new ArrayList<Integer>();

			for (StreamEdge outEdge : streamGraph.getOutEdges(current)) {
				Integer outID = outEdge.getTargetVertex();
				if (isChainable(current, outID)) {
					chainableOutputs.add(outID);
				} else {
					nonChainableOutputs.add(outID);
				}
			}

			for (Integer chainable : chainableOutputs) {
				transitiveOutEdges.addAll(createChain(startNode, chainable));
			}

			for (Integer nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(new Tuple2<Integer, Integer>(current, nonChainable));
				createChain(nonChainable, nonChainable);
			}

			chainedNames.put(current, createChainedName(current, chainableOutputs));

			StreamConfig config = current.equals(startNode) ? createProcessingVertex(startNode)
					: new StreamConfig(new Configuration());

			setVertexConfig(current, config, chainableOutputs, nonChainableOutputs);

			if (current.equals(startNode)) {

				config.setChainStart();
				config.setOutEdgesInOrder(transitiveOutEdges);

				for (Tuple2<Integer, Integer> edge : transitiveOutEdges) {
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
			return new ArrayList<Tuple2<Integer, Integer>>();
		}
	}

	private String createChainedName(Integer vertexID, List<Integer> chainedOutputs) {
		String operatorName = streamGraph.getOperatorName(vertexID);
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<String>();
			for (Integer chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0));
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
			List<Integer> chainableOutputs, List<Integer> nonChainableOutputs) {

		config.setVertexID(vertexID);
		config.setBufferTimeout(streamGraph.getBufferTimeout(vertexID));

		config.setTypeSerializerIn1(streamGraph.getInSerializer1(vertexID));
		config.setTypeSerializerIn2(streamGraph.getInSerializer2(vertexID));
		config.setTypeSerializerOut1(streamGraph.getOutSerializer1(vertexID));
		config.setTypeSerializerOut2(streamGraph.getOutSerializer2(vertexID));

		config.setUserInvokable(streamGraph.getInvokable(vertexID));
		config.setOutputSelectors(streamGraph.getOutputSelector(vertexID));

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);
		config.setStateMonitoring(streamGraph.isMonitoringEnabled());

		Class<? extends AbstractInvokable> vertexClass = streamGraph.getJobVertexClass(vertexID);

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getIterationID(vertexID));
			config.setIterationWaitTime(streamGraph.getIterationTimeout(vertexID));
		}

		List<Integer> allOutputs = new ArrayList<Integer>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		for (Integer output : allOutputs) {
			config.setSelectedNames(output, streamGraph.getEdge(vertexID, output).getSelectedNames());
		}

		vertexConfigs.put(vertexID, config);
	}

	private <T> void connect(Integer headOfChain, Tuple2<Integer, Integer> edge) {

		Integer upStreamvertexID = edge.f0;
		Integer downStreamvertexID = edge.f1;

		int outputIndex = streamGraph.getOutEdges(upStreamvertexID).indexOf(downStreamvertexID);

		AbstractJobVertex headVertex = streamVertices.get(headOfChain);
		AbstractJobVertex downStreamVertex = streamVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());
		StreamConfig upStreamConfig = headOfChain.equals(upStreamvertexID) ? new StreamConfig(
				headVertex.getConfiguration()) : chainedConfigs.get(headOfChain).get(
				upStreamvertexID);

//		List<Integer> outEdgeIndexList = streamGraph.getOutEdgeTypes(upStreamvertexID);
		int numOfInputs = downStreamConfig.getNumberOfInputs();

		downStreamConfig.setInputIndex(numOfInputs++, streamGraph.getEdge(upStreamvertexID, downStreamvertexID).getTypeNumber());
		downStreamConfig.setNumberOfInputs(numOfInputs);

		StreamPartitioner<?> partitioner = streamGraph.getEdge(upStreamvertexID, downStreamvertexID).getPartitioner();

		upStreamConfig.setPartitioner(downStreamvertexID, partitioner);

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

	private boolean isChainable(Integer vertexID, Integer outName) {

		StreamInvokable<?, ?> headInvokable = streamGraph.getInvokable(vertexID);
		StreamInvokable<?, ?> outInvokable = streamGraph.getInvokable(outName);

		return streamGraph.getInEdges(outName).size() == 1
				&& outInvokable != null
				&& outInvokable.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headInvokable.getChainingStrategy() == ChainingStrategy.HEAD || headInvokable
						.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& streamGraph.getEdge(vertexID, outName).getPartitioner().getStrategy() == PartitioningStrategy.FORWARD
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
