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

package org.apache.flink.streaming.api.graph;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator.ChainingStrategy;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	private StreamGraph streamGraph;

	private Map<Integer, JobVertex> jobVertices;
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
		this.jobVertices = new HashMap<Integer, JobVertex>();
		this.builtVertices = new HashSet<Integer>();
		this.chainedConfigs = new HashMap<Integer, Map<Integer, StreamConfig>>();
		this.vertexConfigs = new HashMap<Integer, StreamConfig>();
		this.chainedNames = new HashMap<Integer, String>();
		this.physicalEdgesInOrder = new ArrayList<StreamEdge>();
	}

	public JobGraph createJobGraph(String jobName) {
		jobGraph = new JobGraph(jobName);

		// make sure that all vertices start immediately
		jobGraph.setScheduleMode(ScheduleMode.ALL);

		init();

		setChaining();

		setPhysicalEdges();

		setSlotSharing();
		
		configureCheckpointing();

		configureExecutionRetries();

		try {
			InstantiationUtil.writeObjectToConfig(this.streamGraph.getExecutionConfig(), this.jobGraph.getJobConfiguration(), ExecutionConfig.CONFIG_KEY);
		} catch (IOException e) {
			throw new RuntimeException("Config object could not be written to Job Configuration: ", e);
		}
		
		return jobGraph;
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

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
		for (Integer sourceName : streamGraph.getSourceIDs()) {
			createChain(sourceName, sourceName);
		}
	}

	private List<StreamEdge> createChain(Integer startNode, Integer current) {

		if (!builtVertices.contains(startNode)) {

			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			for (StreamEdge outEdge : streamGraph.getStreamNode(current).getOutEdges()) {
				if (isChainable(outEdge)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(createChain(startNode, chainable.getTargetId()));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId());
			}

			chainedNames.put(current, createChainedName(current, chainableOutputs));

			StreamConfig config = current.equals(startNode) ? createProcessingVertex(startNode)
					: new StreamConfig(new Configuration());

			setVertexConfig(current, config, chainableOutputs, nonChainableOutputs);

			if (current.equals(startNode)) {

				config.setChainStart();
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getStreamNode(current).getOutEdges());

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
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<String>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			String returnOperatorName = operatorName + " -> ("
					+ StringUtils.join(outputChainedNames, ", ") + ")";
			return returnOperatorName;
		} else if (chainedOutputs.size() == 1) {
			String returnOperatorName = operatorName + " -> "
					+ chainedNames.get(chainedOutputs.get(0).getTargetId());
			return returnOperatorName;
		} else {
			return operatorName;
		}

	}

	private StreamConfig createProcessingVertex(Integer vertexID) {

		JobVertex jobVertex = new JobVertex(chainedNames.get(vertexID));
		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		jobVertex.setInvokableClass(vertex.getJobVertexClass());

		int parallelism = vertex.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, vertexID);
		}

		if (vertex.getInputFormat() != null) {
			jobVertex.setInputSplitSource(vertex.getInputFormat());
		}

		jobVertices.put(vertexID, jobVertex);
		builtVertices.add(vertexID);
		jobGraph.addVertex(jobVertex);

		StreamConfig retConfig = new StreamConfig(jobVertex.getConfiguration());
		retConfig.setOperatorName(chainedNames.get(vertexID));
		return retConfig;
	}

	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut1(vertex.getTypeSerializerOut());

		config.setStreamOperator(vertex.getOperator());
		config.setOutputSelectorWrapper(vertex.getOutputSelectorWrapper());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setCheckpointingEnabled(streamGraph.isCheckpointingEnabled());
		if (streamGraph.isCheckpointingEnabled()) {
			config.setCheckpointMode(streamGraph.getCheckpointingMode());
			config.setStateHandleProvider(streamGraph.getStateHandleProvider());
		} else {
			// the at least once input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner((KeySelector<?, Serializable>) vertex.getStatePartitioner());

		
		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		List<StreamEdge> allOutputs = new ArrayList<StreamEdge>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetId();

		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();
		if (partitioner instanceof ForwardPartitioner) {
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
		StreamNode upStreamVertex = edge.getSourceVertex();
		StreamNode downStreamVertex = edge.getTargetVertex();

		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.getSlotSharingID() == downStreamVertex.getSlotSharingID()
				&& upStreamVertex.getSlotSharingID() != -1
				&& (outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS ||
					outOperator.getChainingStrategy() == ChainingStrategy.FORCE_ALWAYS)
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS ||
					headOperator.getChainingStrategy() == ChainingStrategy.FORCE_ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner || downStreamVertex
						.getParallelism() == 1)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& (streamGraph.isChainingEnabled() ||
					outOperator.getChainingStrategy() == ChainingStrategy.FORCE_ALWAYS);
	}

	private void setSlotSharing() {

		Map<Integer, SlotSharingGroup> slotSharingGroups = new HashMap<Integer, SlotSharingGroup>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			int slotSharingID = streamGraph.getStreamNode(entry.getKey()).getSlotSharingID();

			if (slotSharingID != -1) {
				SlotSharingGroup group = slotSharingGroups.get(slotSharingID);
				if (group == null) {
					group = new SlotSharingGroup();
					slotSharingGroups.put(slotSharingID, group);
				}
				entry.getValue().setSlotSharingGroup(group);
			}
		}

		for (Tuple2<StreamNode, StreamNode> pair : streamGraph.getIterationSourceSinkPairs()) {

			CoLocationGroup ccg = new CoLocationGroup();

			JobVertex source = jobVertices.get(pair.f0.getId());
			JobVertex sink = jobVertices.get(pair.f1.getId());

			ccg.addVertex(source);
			ccg.addVertex(sink);
			source.updateCoLocationGroup(ccg);
			sink.updateCoLocationGroup(ccg);
		}

	}
	
	private void configureCheckpointing() {
		if (streamGraph.isCheckpointingEnabled()) {
			long interval = streamGraph.getCheckpointingInterval();
			if (interval < 1) {
				throw new IllegalArgumentException("The checkpoint interval must be positive");
			}

			// collect the vertices that receive "trigger checkpoint" messages.
			// currently, these are all the sources
			List<JobVertexID> triggerVertices = new ArrayList<JobVertexID>();

			// collect the vertices that need to acknowledge the checkpoint
			// currently, these are all vertices
			List<JobVertexID> ackVertices = new ArrayList<JobVertexID>(jobVertices.size());

			// collect the vertices that receive "commit checkpoint" messages
			// currently, these are all certices
			List<JobVertexID> commitVertices = new ArrayList<JobVertexID>();
			
			
			for (JobVertex vertex : jobVertices.values()) {
				if (vertex.isInputVertex()) {
					triggerVertices.add(vertex.getID());
				}
				// TODO: add check whether the user function implements the checkpointing interface
				commitVertices.add(vertex.getID());
				ackVertices.add(vertex.getID());
			}

			JobSnapshottingSettings settings = new JobSnapshottingSettings(
					triggerVertices, ackVertices, commitVertices, interval);
			jobGraph.setSnapshotSettings(settings);

			// if the user enabled checkpointing, the default number of exec retries is infinitive.
			int executionRetries = streamGraph.getExecutionConfig().getNumberOfExecutionRetries();
			if(executionRetries == -1) {
				streamGraph.getExecutionConfig().setNumberOfExecutionRetries(Integer.MAX_VALUE);
			}
		}
	}

	private void configureExecutionRetries() {
		int executionRetries = streamGraph.getExecutionConfig().getNumberOfExecutionRetries();
		if (executionRetries != -1) {
			jobGraph.setNumberOfExecutionRetries(executionRetries);
		} else {
			// if the user didn't configure anything, the number of retries is 0.
			jobGraph.setNumberOfExecutionRetries(0);
		}
	}
}
