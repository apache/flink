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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.FormatUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.MultiInputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.OperatorDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorEdgeDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamNode.ReadPriority;
import org.apache.flink.streaming.api.graph.util.CursorableLinkedList;
import org.apache.flink.streaming.api.graph.util.CursorableLinkedList.Cursor;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.ArbitraryInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigCache;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	/**
	 * Restart delay used for the FixedDelayRestartStrategy in case checkpointing was enabled but
	 * no restart strategy has been specified.
	 */
	private static final long DEFAULT_RESTART_DELAY = 0L;

	/**
	 * Maps job vertex id to stream node ids.
	 */
	public static final String JOB_VERTEX_TO_STREAM_NODE_MAP = "jobVertexToStreamNodeMap";

	// ------------------------------------------------------------------------

	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return new StreamingJobGraphGenerator(streamGraph).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	private final JobGraph jobGraph;

	/**
	 * The mapping of chained node to JobVertex.
	 */
	private final Map<Integer, JobVertex> nodeToJobVertexMap;

	/**
	 * The output edge list of all chains which is global sorted by topological order.
	 */
	private final List<StreamEdge> transitiveOutEdges;

	/**
	 * The mapping of starting traversal head node to all nodes of chain.
	 */
	private final Map<Integer, List<Integer>> chainedNodeIdsMap;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Collections.singletonList(new StreamGraphUserHashHasher());

		this.nodeToJobVertexMap = new HashMap<>();
		this.transitiveOutEdges = new ArrayList<>();
		this.chainedNodeIdsMap = new HashMap<>();

		this.jobGraph = new JobGraph(streamGraph.getJobName());
	}

	private JobGraph createJobGraph() {

		// add custom configuration to the job graph
		jobGraph.addCustomConfiguration(streamGraph.getCustomConfiguration());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		setChaining(hashes, legacyHashes);

		connectEdges();

		setSlotSharing();

		configureCheckpointing();

		setSchedulerConfiguration();

		// add registered cache file into job configuration
		for (Tuple2<String, DistributedCache.DistributedCacheEntry> e : streamGraph.getCachedFiles()) {
			jobGraph.addUserArtifact(e.f0, e.f1);
		}

		// set the ExecutionConfig last when it has been finalized
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	/**
	 * Set parameters for job scheduling. Schedulers may leverage these parameters to schedule tasks.
	 */
	private void setSchedulerConfiguration() {
		Configuration configuration = jobGraph.getSchedulingConfiguration();

		setVertexToStreamNodesMap(configuration);
		configuration.addAll(streamGraph.getCustomConfiguration());
	}

	private void setVertexToStreamNodesMap(Configuration configuration) {
		Map<JobVertexID, List<Integer>> vertexToStreamNodeIds = new HashMap<>();
		for (Map.Entry<Integer, List<Integer>> entry : chainedNodeIdsMap.entrySet()) {
			JobVertex jobVertex = nodeToJobVertexMap.get(entry.getKey());
			vertexToStreamNodeIds.put(jobVertex.getID(), entry.getValue() == null ? Collections.emptyList() : entry.getValue());
		}

		try {
			InstantiationUtil.writeObjectToConfig(vertexToStreamNodeIds, configuration, JOB_VERTEX_TO_STREAM_NODE_MAP);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {

		// sort all sources for the following purpose:
		// 1.Processes StreamSource prior to StreamSourceV2.
		// 2.Stabilizes the topology of the generated job graph.
		final List<Integer> sortedSourceIDs = streamGraph.getSourceIDs().stream()
			.sorted(
				Comparator.comparing((Integer id) -> {
					StreamOperator<?> operator = streamGraph.getStreamNode(id).getOperator();
					return operator == null || operator instanceof StreamSource ? 0 : 1;
				}).thenComparingInt(id -> id))
			.collect(Collectors.toList());

		// sort nodes in a topological order with breadth-first
		final List<ChainingStreamNode> sortedChainingNodes = sortTopologicalNodes(streamGraph, sortedSourceIDs);

		final Map<Integer, ChainingStreamNode> chainingNodeMap = sortedChainingNodes.stream()
				.collect(Collectors.toMap(ChainingStreamNode::getNodeId, (o) -> o));

		// set depth-first number for each chaining node for sorting chained edges and transitive-out edges
		final SequenceGenerator depthFirstSequenceGenerator = new SequenceGenerator();
		final Set<Integer> visitedNodeSet = new HashSet<>();
		for (Integer sourceNodeId : sortedSourceIDs) {
			setDepthFirstNumber(sourceNodeId, chainingNodeMap, streamGraph, depthFirstSequenceGenerator, visitedNodeSet);
		}

		// split chains using the following strategies while chaining is enabled:
		// 1.Chained rules including equal parallelism, etc.
		// 2.No deadlock occur while operators use dynamic selection reading on multi-head chaining.
		// 3.No directed cycles occur in the generated job graph.
		final Map<Integer, ChainingStreamNode> chainedNodeMap;
		if (streamGraph.isChainingEnabled()) {
			splitChain(sortedChainingNodes, chainingNodeMap);
			chainedNodeMap = chainingNodeMap;
		} else {
			chainedNodeMap = null;
		}

		// create all job vertices
		CreatingChainIntermediateStorager intermediateStorager = new CreatingChainIntermediateStorager();
		for (ChainingStreamNode chainingNode : sortedChainingNodes) {
			Integer startNodeId = chainingNode.getNodeId();

			if (createChain(startNodeId, startNodeId, new SequenceGenerator(), chainedNodeMap, hashes, legacyHashes, intermediateStorager)) {
				for (Integer nodeId : intermediateStorager.chainedNodeIdsInOrder) {
					nodeToJobVertexMap.put(nodeId, intermediateStorager.createdVertex);
				}
				transitiveOutEdges.addAll(intermediateStorager.chainOutEdgesInOrder);
				chainedNodeIdsMap.put(startNodeId, new ArrayList<>(intermediateStorager.chainedNodeIdsInOrder));

				// add the job vertex to JobGraph
				jobGraph.addVertex(intermediateStorager.createdVertex);
			}

			intermediateStorager.resetForNewChain();
		}

		// Sorts output edges of all job vertices.
		// The sorting policy must be consistent with {@code CreatingChainIntermediateStorager.chainInEdgesInOrder}
		// and {@code CreatingChainIntermediateStorager.chainOutEdgesInOrder} .
		transitiveOutEdges.sort(
			Comparator.comparingInt((StreamEdge o) -> chainingNodeMap.get(o.getTargetId()).getDepthFirstNumber())
				.thenComparingInt((o) -> streamGraph.getStreamNode(o.getTargetId()).getInEdges().indexOf(o))
		);
	}

	/**
	 * Sorts nodes in a topological order with breadth-first and creates a shadow graph for chaining.
	 *
	 * @param sortedSourceIDs The sorted list of id for all source nodes.
	 * @return The list of sorted chaining nodes.
	 */
	static List<ChainingStreamNode> sortTopologicalNodes(StreamGraph streamGraph, List<Integer> sortedSourceIDs) {
		final List<ChainingStreamNode> sortedChainingNodes = new ArrayList<>();

		final Deque<Integer> visitedNonZeroInputNodes = new ArrayDeque<>();
		final Map<Integer, Integer[]> remainingInputNumMap = new HashMap<>();
		for (Integer sourceNodeId : sortedSourceIDs) {
			visitedNonZeroInputNodes.add(sourceNodeId);
			remainingInputNumMap.put(sourceNodeId, new Integer[] {0, -1});
		}

		int iterationNumber = -1;
		final SequenceGenerator topologicalOrderSeq =  new SequenceGenerator();

		int size;
		final Set<Integer> dedupSet = new HashSet<>();
		while ((size = visitedNonZeroInputNodes.size()) > 0) {
			iterationNumber++;
			int remainingNumIndex1 = iterationNumber % 2, remainingNumIndex2 = (iterationNumber + 1) % 2;

			dedupSet.clear();
			boolean hasZeroInputNode = false;
			for (int i = 0; i < size; i++) {
				Integer currentNodeId = visitedNonZeroInputNodes.pollFirst();
				Integer remainingInputNum = remainingInputNumMap.get(currentNodeId)[remainingNumIndex1];
				if (remainingInputNum == 0) {
					StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
					ChainingStreamNode chainingNode = new ChainingStreamNode(currentNodeId,	currentNode.getInEdges().size(),
							topologicalOrderSeq.get(), iterationNumber);
					sortedChainingNodes.add(chainingNode);

					for (StreamEdge outEdge : currentNode.getOutEdges()) {
						Integer targetId = outEdge.getTargetId();

						if (!dedupSet.contains(targetId)) {
							visitedNonZeroInputNodes.addLast(targetId);

							Integer[] remainingInputNums = remainingInputNumMap.get(targetId);
							if (remainingInputNums == null) {
								remainingInputNums = new Integer[]{-1, -1};
								remainingInputNums[remainingNumIndex2] = streamGraph.getStreamNode(targetId).getInEdges().size() - 1;

								remainingInputNumMap.put(targetId, remainingInputNums);
							} else {
								remainingInputNums[remainingNumIndex2] = remainingInputNums[remainingNumIndex1] - 1;
							}

							dedupSet.add(targetId);
						} else {
							remainingInputNumMap.get(targetId)[remainingNumIndex2]--;
						}
					}

					remainingInputNumMap.remove(currentNodeId);
					hasZeroInputNode = true;
				} else if (remainingInputNum > 0) {
					if (!dedupSet.contains(currentNodeId)) {
						visitedNonZeroInputNodes.addLast(currentNodeId);

						Integer[] remainingInputNums = remainingInputNumMap.get(currentNodeId);
						remainingInputNums[remainingNumIndex2] = remainingInputNums[remainingNumIndex1];

						dedupSet.add(currentNodeId);
					}
				} else {
					throw new RuntimeException("remainingInputNum for the node (id: " + currentNodeId + ") should be greater than 0");
				}
			}

			if (!hasZeroInputNode) {
				break;
			}
		}

		if (sortedChainingNodes.size() < streamGraph.getStreamNodes().size()) {
			throw new RuntimeException("The stream graph is cyclic.");
		}

		return sortedChainingNodes;
	}

	/**
	 * Sets depth-first number for each chaining node.
	 */
	static void setDepthFirstNumber(Integer nodeId,
		Map<Integer, ChainingStreamNode> chainingNodeMap,
		StreamGraph streamGraph,
		SequenceGenerator depthFirstSequenceGenerator,
		Set<Integer> visitedNodeSet) {

		if (visitedNodeSet.contains(nodeId)) {
			return;
		} else {
			visitedNodeSet.add(nodeId);
		}

		StreamNode currentNode = streamGraph.getStreamNode(nodeId);
		for (StreamEdge outEdge : currentNode.getOutEdges()) {
			setDepthFirstNumber(outEdge.getTargetId(), chainingNodeMap, streamGraph, depthFirstSequenceGenerator, visitedNodeSet);
		}

		chainingNodeMap.get(nodeId).setDepthFirstNumber(depthFirstSequenceGenerator.get());
	}

	/**
	 * Splits chains using the strategies as follows.
	 * 1.Chained rules including equal parallelism, etc.
	 * 2.No deadlock occur while operators use dynamic selection reading on multi-head chaining.
	 * 3.No directed cycles occur in the generated job graph.
	 *
	 * @param sortedChainingNodes The list of sorted chaining nodes.
	 * @param chainingNodeMap The map of all chaining nodes.
	 */
	private void splitChain(List<ChainingStreamNode> sortedChainingNodes, Map<Integer, ChainingStreamNode> chainingNodeMap) {

		// split up initial chains according to chained rules
		splitUpInitialChains(chainingNodeMap, sortedChainingNodes, streamGraph);

		if (streamGraph.isMultiHeadChainMode()) {
			// break off chain to avoid deadlock
			breakOffChainForNoDeadlock(chainingNodeMap, sortedChainingNodes, streamGraph);

			// break off chain to avoid directed cycles in the job graph
			breakOffChainForAcyclicJobGraph(chainingNodeMap, sortedChainingNodes, streamGraph);
		}
	}

	static void splitUpInitialChains(
		Map<Integer, ChainingStreamNode> chainingNodeMap,
		List<ChainingStreamNode> sortedChainingNodes,
		StreamGraph streamGraph) {

		for (ChainingStreamNode currentChainingNode : sortedChainingNodes) {
			StreamNode currentNode = streamGraph.getStreamNode(currentChainingNode.getNodeId());

			// StreamSource and iteration-head nodes are not allowed to do multi-head chaining
			if (currentNode.getInEdges().size() == 0) {
				StreamOperator<?> operator = currentNode.getOperator();
				currentChainingNode.setAllowMultiHeadChaining(
						operator == null || operator instanceof StreamSource ? Boolean.FALSE : Boolean.TRUE);
			}

			for (StreamEdge edge : currentNode.getOutEdges()) {
				ChainingStreamNode downstreamChainingNode = chainingNodeMap.get(edge.getTargetId());

				downstreamChainingNode.chainTo(currentChainingNode,
						edge,
						streamGraph.getStreamNode(edge.getSourceId()),
						streamGraph.getStreamNode(edge.getTargetId()),
						streamGraph.isMultiHeadChainMode(),
						streamGraph.isChainEagerlyEnabled(),
						streamGraph.getExecutionConfig().getExecutionMode());
			}
		}
	}

	static void breakOffChainForNoDeadlock(
		Map<Integer, ChainingStreamNode> chainingNodeMap,
		List<ChainingStreamNode> sortedChainingNodes,
		StreamGraph streamGraph) {

		// infer the read priority of each edge from bottom to top of the topology
		inferReadPriority(chainingNodeMap, sortedChainingNodes, streamGraph);

		// break chain in which may occur deadlock
		for (ChainingStreamNode chainingNode : sortedChainingNodes) {
			Integer nodeId = chainingNode.getNodeId();
			StreamNode node = streamGraph.getStreamNode(nodeId);

			if (!chainingNode.isReadPriorityConflicting()) {
				continue;
			}

			for (StreamEdge inEdge : node.getInEdges()) {
				Integer sourceId = inEdge.getSourceId();
				ReadPriority readPriority = chainingNode.getReadPriority(sourceId);
				if (!ReadPriority.HIGHER.equals(readPriority)) {
					if (needToBreakOffChain(inEdge, chainingNodeMap, streamGraph)) {
						chainingNode.removeChainableToNode(sourceId);
					}
				}
			}
		}
	}

	static void inferReadPriority(
			Map<Integer, ChainingStreamNode> chainingNodeMap,
			List<ChainingStreamNode> sortedChainingNodes,
			StreamGraph streamGraph) {

		// infer the read priority of each edge from bottom to top of the topology

		List<StreamEdge> lackPriorEdges = new ArrayList<>();
		for (int i = sortedChainingNodes.size() - 1; i >= 0; i--) {
			ChainingStreamNode chainingNode = sortedChainingNodes.get(i);
			Integer nodeId = chainingNode.getNodeId();
			StreamNode node = streamGraph.getStreamNode(nodeId);

			for (StreamEdge inEdge : node.getInEdges()) {
				if (inEdge.getDataExchangeMode() == DataExchangeMode.BATCH) {
					lackPriorEdges.add(inEdge);
					continue;
				}

				Integer upstreamNodeId = inEdge.getSourceId();
				ChainingStreamNode upstreamChainingNode = chainingNodeMap.get(upstreamNodeId);
				ReadPriority readPriority = node.getReadPriorityHint(inEdge);
				if (readPriority == null) {
					readPriority = chainingNode.getTransitivePriority();
				} else {
					ReadPriority downPriority = chainingNode.getTransitivePriority();
					if (downPriority != null && !readPriority.equals(downPriority)) {
						readPriority = ReadPriority.DYNAMIC;
					}
				}
				if (readPriority != null) {
					chainingNode.setReadPriority(upstreamNodeId, readPriority);
					upstreamChainingNode.setDownPriority(nodeId, readPriority);
				} else {
					lackPriorEdges.add(inEdge);
				}
			}
		}

		for (int i = 0; i < lackPriorEdges.size(); i++) {
			StreamEdge edge = lackPriorEdges.get(i);

			Integer nodeId = edge.getSourceId();
			Integer downstreamNodeId = edge.getTargetId();
			ChainingStreamNode chainingNode = chainingNodeMap.get(nodeId);
			ChainingStreamNode downstreamChainingNode = chainingNodeMap.get(downstreamNodeId);

			ReadPriority readPriority = chainingNode.getTransitivePriority();
			if (readPriority == null) {
				readPriority = ReadPriority.HIGHER;
			}
			downstreamChainingNode.setReadPriority(nodeId, readPriority);
			chainingNode.setDownPriority(downstreamNodeId, readPriority);
		}
	}

	private static boolean needToBreakOffChain(
			final StreamEdge origEdge,
			final Map<Integer, ChainingStreamNode> chainingNodeMap,
			final StreamGraph streamGraph) {

		Integer origNodeId = origEdge.getTargetId();
		Integer origUpstreamId = origEdge.getSourceId();
		boolean isOrigEdgeBroken = !(chainingNodeMap.get(origNodeId).isChainTo(origUpstreamId));

		boolean isInputBreakCondMet = false;
		boolean isPriorityConflictCondMet = false;
		boolean isDamCondMet = false;

		Deque<Pair<StreamEdge, Boolean>> edgeQueue = new ArrayDeque<>();
		edgeQueue.addLast(new ImmutablePair<>(origEdge, isOrigEdgeBroken));
		int currentBreakNum = isOrigEdgeBroken ? 1 : 0;
		while (edgeQueue.size() > 0) {
			if (!isInputBreakCondMet && currentBreakNum == edgeQueue.size()) {
				isInputBreakCondMet = true;
			}

			Pair<StreamEdge, Boolean> pair = edgeQueue.pollFirst();
			StreamEdge currentEdge = pair.getLeft();
			Integer upstreamNodeId = currentEdge.getSourceId();
			boolean isDownBroken = pair.getRight();
			if (isDownBroken) {
				currentBreakNum--;
			}

			if (isInputBreakCondMet && !isDamCondMet) {
				break;
			}

			if (!isDamCondMet && !isDownBroken) {
				if (DamBehavior.FULL_DAM.equals(currentEdge.getDamBehavior())) {
					isDamCondMet = true;
				}
			}

			if (isDamCondMet && chainingNodeMap.get(upstreamNodeId).isDownPriorityConflicting()) {
				isPriorityConflictCondMet = true;
				break;
			}

			StreamNode upstreamNode = streamGraph.getStreamNode(upstreamNodeId);
			for (StreamEdge nextInEdge : upstreamNode.getInEdges()) {
				if (!isDownBroken) {
					isDownBroken = !(chainingNodeMap.get(nextInEdge.getTargetId()).isChainTo(nextInEdge.getSourceId()));
				}
				if (isDownBroken) {
					currentBreakNum++;
				}
				edgeQueue.addLast(new ImmutablePair<>(nextInEdge, isDownBroken));
			}
		}

		return (isDamCondMet && isPriorityConflictCondMet);
	}

	static void breakOffChainForAcyclicJobGraph(Map<Integer, ChainingStreamNode> chainingNodeMap,
		List<ChainingStreamNode> sortedChainingNodes,
		StreamGraph streamGraph) {

		if (chainingNodeMap.size() == 0) {
			return;
		}

		// match chained nodes to ensure that the job graph is acyclic
		Map<Integer, CoarsenedNode> coarsenedNodeMap = chainingNodeMap.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, (o) -> {
					Integer nodeId = o.getKey();
					return new CoarsenedNode(streamGraph.getStreamNode(nodeId), chainingNodeMap.get(nodeId));
				}));

		CursorableLinkedList<CoarsenedNode> sortedCoarsenedNodes = new CursorableLinkedList<>();
		for (ChainingStreamNode chainingNode : sortedChainingNodes) {
			sortedCoarsenedNodes.add(coarsenedNodeMap.get(chainingNode.getNodeId()));
		}

		Map<Integer, Cursor<CoarsenedNode>> cursorMap = new HashMap<>();

		Cursor<CoarsenedNode> cursor = sortedCoarsenedNodes.cursor();
		CoarsenedNode coarsenedNode = cursor.next();
		while (true) {
			Integer coarsenedId = coarsenedNode.getId();
			cursorMap.put(coarsenedId, sortedCoarsenedNodes.cursor(cursor));

			CoarsenedNode nextCoarsenedNode = null;
			if (cursor.hasNext()) {
				nextCoarsenedNode = cursor.next();
			}

			Set<Integer> predCoarsenedNodes = coarsenedNode.getPredecessorsNodes().keySet();
			for (Integer predCoarsenedId : predCoarsenedNodes) {
				if (!coarsenedNode.getPredecessorsNodes().get(predCoarsenedId)) {
					continue;
				}

				CoarsenedNode predCoarsenedNode = coarsenedNodeMap.get(predCoarsenedId);

				Cursor<CoarsenedNode> newCursor = tryToMergeCoarsenedNode(sortedCoarsenedNodes, coarsenedNode, predCoarsenedNode, cursorMap);
				if (newCursor != null) {
					predCoarsenedNode.merge(coarsenedNode, coarsenedNodeMap);
					cursorMap.put(predCoarsenedId, newCursor);

					coarsenedNodeMap.remove(coarsenedId);
					cursorMap.remove(coarsenedId);

					coarsenedNode = predCoarsenedNode;
					coarsenedId = predCoarsenedId;
				}
			}

			if (nextCoarsenedNode != null) {
				coarsenedNode = nextCoarsenedNode;
			} else {
				break;
			}
		}

		// break off edges which can't be chained by ensuring acyclic
		Map<Integer, Integer> streamNodeIdToCoarsenedIdMap = new HashMap<>();
		cursor = sortedCoarsenedNodes.cursor();
		while (cursor.hasNext()) {
			coarsenedNode = cursor.next();
			Integer coarsenedId = coarsenedNode.getId();
			for (Integer streamNodeId : coarsenedNode.getOriginalNodes()) {
				streamNodeIdToCoarsenedIdMap.put(streamNodeId, coarsenedId);
			}
		}

		for (ChainingStreamNode chainingNode : sortedChainingNodes) {
			Integer nodeId = chainingNode.getNodeId();
			Integer coarsenedId = streamNodeIdToCoarsenedIdMap.get(nodeId);
			StreamNode node = streamGraph.getStreamNode(nodeId);
			for (StreamEdge edge : node.getInEdges()) {
				Integer sourceId = edge.getSourceId();
				if (chainingNode.isChainTo(sourceId) &&
						!streamNodeIdToCoarsenedIdMap.get(sourceId).equals(coarsenedId)) {
					chainingNode.removeChainableToNode(sourceId);
				}
			}

			chainingNode.setCoarsenedId(coarsenedId);
		}
	}

	private static Cursor<CoarsenedNode> tryToMergeCoarsenedNode(
		CursorableLinkedList<CoarsenedNode> sortedCoarsenedNodes,
		CoarsenedNode coarsenedNode,
		CoarsenedNode predecessorNode,
		Map<Integer, Cursor<CoarsenedNode>> cursorMap) {

		Integer coarsenedId = coarsenedNode.getId();
		Integer predecessorId = predecessorNode.getId();

		Cursor<CoarsenedNode> cursor1 = sortedCoarsenedNodes.cursor(cursorMap.get(predecessorId));
		CoarsenedNode currentNode = cursor1.getValue();
		Map<Integer, Boolean> predecessorNodes = coarsenedNode.getPredecessorsNodes();
		int pos1 = 0, index = 0;
		while (true) {
			Integer currentNodeId = currentNode.getId();
			if (predecessorNodes.containsKey(currentNodeId)) {
				pos1 = index;
			}

			if (currentNodeId.equals(coarsenedId)) {
				break;
			}

			if (!cursor1.hasNext()) {
				throw new IllegalStateException("An internal error is occurred.");
			}
			index++;
			currentNode = cursor1.next();
		}

		Cursor<CoarsenedNode> cursor2 = sortedCoarsenedNodes.cursor(cursorMap.get(predecessorId));
		int pos2 = 0;
		Set<Integer> successorNodes = predecessorNode.getSucessorNodes();
		while (cursor2.hasNext()) {
			pos2++;
			currentNode = cursor2.next();
			Integer currentNodeId = currentNode.getId();
			if (successorNodes.contains(currentNodeId)) {
				break;
			}

			if (currentNodeId.equals(coarsenedId)) {
				throw new IllegalStateException("An internal error is occurred");
			}
		}

		if (pos2 > pos1) {
			Cursor<CoarsenedNode> resultCursor = cursorMap.get(predecessorId);
			if (pos2 > 1) {
				resultCursor.moveNodeTo(cursor2);
			}
			cursorMap.get(coarsenedId).remove();
			return resultCursor;
		}

		return null;
	}

	/**
	 * Creates task chains from partitioned topology of the stream graph.
	 */
	private boolean createChain(
			Integer startNodeId,
			Integer currentNodeId,
			SequenceGenerator chainIndexGenerator,
			@Nullable Map<Integer, ChainingStreamNode> chainedNodeMap,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			CreatingChainIntermediateStorager storager) {

		if (storager.allBuiltNodes.contains(currentNodeId)) {
			return false;
		}

		storager.allBuiltNodes.add(currentNodeId);

		// current node related
		StreamNode currentStreamNode = streamGraph.getStreamNode(currentNodeId);

		int chainIndex = chainIndexGenerator.get();
		byte[] primaryHashBytes = hashes.get(currentNodeId);
		boolean isHeadNode = (chainedNodeMap == null || chainedNodeMap.get(currentNodeId).isChainHeadNode());

		List<StreamEdge> chainedOutputs = new ArrayList<>();
		List<StreamEdge> nonChainedOutputs = new ArrayList<>();

		/* Traverses from the current node, first going down and up. */

		// going down
		for (StreamEdge outEdge : currentStreamNode.getOutEdges()) {
			Integer downstreamNodeId = outEdge.getTargetId();
			ChainingStreamNode downstreamNode = (chainedNodeMap == null) ? null : chainedNodeMap.get(downstreamNodeId);

			if (chainedNodeMap == null || !downstreamNode.isChainTo(currentNodeId)) {
				nonChainedOutputs.add(outEdge);
				continue;
			}

			chainedOutputs.add(outEdge);

			createChain(
				startNodeId,
				downstreamNodeId,
				chainIndexGenerator,
				chainedNodeMap,
				hashes,
				legacyHashes,
				storager);
		}

		// generate chained name of the current node
		storager.chainedNameMap.put(currentNodeId, makeChainedName(currentStreamNode.getOperatorName(), chainedOutputs, storager.chainedNameMap));

		// going up
		if (chainedNodeMap != null) {
			for (StreamEdge inEdge : currentStreamNode.getInEdges()) {
				Integer upstreamNodeId = inEdge.getSourceId();
				ChainingStreamNode currentNode = chainedNodeMap.get(currentNodeId);
				if (!currentNode.isChainTo(upstreamNodeId)) {
					continue;
				}

				createChain(
					startNodeId,
					upstreamNodeId,
					chainIndexGenerator,
					chainedNodeMap,
					hashes,
					legacyHashes,
					storager);
					}
		}

		/* The traversal is finished. */

		// create StreamConfig for the current node
		StreamConfig currentNodeConfig = new StreamConfig(new Configuration());
		OperatorID currentOperatorID = new OperatorID(primaryHashBytes);

		if (isHeadNode) {
			currentNodeConfig.setChainStart();
		}
		currentNodeConfig.setChainIndex(chainIndex);
		currentNodeConfig.setOperatorName(currentStreamNode.getOperatorName());
		currentNodeConfig.setOperatorID(currentOperatorID);
		if (chainedOutputs.isEmpty()) {
			currentNodeConfig.setChainEnd();
		}

		List<StreamEdge> nonChainedInputs = new ArrayList<>();
		for (StreamEdge inEdge : currentStreamNode.getInEdges()) {
			if (chainedNodeMap == null) {
				nonChainedInputs.add(inEdge);
			} else {
				ChainingStreamNode currentNode = chainedNodeMap.get(currentNodeId);
				if (!currentNode.isChainTo(inEdge.getSourceId())) {
					nonChainedInputs.add(inEdge);
				}
			}
		}

		setupNodeConfig(currentNodeId, nonChainedInputs, chainedOutputs, nonChainedOutputs, streamGraph, currentNodeConfig);

		// compute and store chained data
		storager.chainedConfigMap.put(currentNodeId, currentNodeConfig);
		storager.chainedNodeIdsInOrder.add(currentNodeId);
		if (isHeadNode) {
			storager.chainedHeadNodeIdsInOrder.add(currentNodeId);
		}

		storager.chainInEdgesInOrder.addAll(nonChainedInputs);
		storager.chainOutEdgesInOrder.addAll(nonChainedOutputs);

		if (currentStreamNode.getOutputFormat() != null) {
			storager.chainOutputFormatMap.put(currentOperatorID, currentStreamNode.getOutputFormat());
		}
		if (currentStreamNode.getInputFormat() != null) {
			storager.chainInputFormatMap.put(currentOperatorID, currentStreamNode.getInputFormat());
		}

		ResourceSpec currentNodeMinResources = currentStreamNode.getMinResources();
		storager.chainedMinResources = (storager.chainedMinResources == null) ?
			currentNodeMinResources : storager.chainedMinResources.merge(currentNodeMinResources);

		ResourceSpec currentNodePreferredResources = currentStreamNode.getPreferredResources();
		storager.chainedPreferredResources = (storager.chainedPreferredResources == null) ?
			currentNodePreferredResources : storager.chainedPreferredResources.merge(currentNodePreferredResources);

		// The chain is end, create job vertex and configuration.
		if (currentNodeId.equals(startNodeId)) {
			if (chainedNodeMap != null) {
				// sort related lists
				storager.chainInEdgesInOrder.sort(
					Comparator.comparingInt((StreamEdge o) -> chainedNodeMap.get(o.getTargetId()).getDepthFirstNumber())
						.thenComparingInt((o) -> streamGraph.getStreamNode(o.getTargetId()).getInEdges().indexOf(o))
				);
				storager.chainOutEdgesInOrder.sort(
					Comparator.comparingInt((StreamEdge o) -> chainedNodeMap.get(o.getTargetId()).getDepthFirstNumber())
						.thenComparingInt((o) -> streamGraph.getStreamNode(o.getTargetId()).getInEdges().indexOf(o))
				);

				storager.chainedNodeIdsInOrder.sort(Comparator.comparingInt((o) -> chainedNodeMap.get(o).getDepthFirstNumber()));
				storager.chainedHeadNodeIdsInOrder.sort(Comparator.comparingInt((o) -> chainedNodeMap.get(o).getTopologicalOrder()));
			}

			storager.createdVertex = createJobVertex(startNodeId, hashes, legacyHashes, storager);

			setupVertexConfig(currentNodeConfig, storager, storager.createdVertex.getConfiguration());
		}

		return true;
	}

	private JobVertex createJobVertex(
			Integer startNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			CreatingChainIntermediateStorager storager) {

		JobVertex jobVertex;

		// generate the id of the job vertex
		byte[] primaryHashBytes = hashes.get(startNodeId);
		if (primaryHashBytes == null) {
			throw new IllegalStateException("Cannot find node hash (nodeId: " + startNodeId + ") . " +
					"Did you generate them before calling this method?");
		}
		JobVertexID jobVertexId = new JobVertexID(primaryHashBytes);

		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			byte[] hash = legacyHash.get(startNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}

		// generate id for chained operators
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();

		for (Integer nodeId : storager.chainedNodeIdsInOrder) {
			byte[] hash = hashes.get(nodeId);
			for (Map<Integer, byte[]> legacyHashMap : legacyHashes) {
				chainedOperatorVertexIds.add(new OperatorID(hash));

				byte[] legacyHash = legacyHashMap.get(nodeId);
				userDefinedChainedOperatorVertexIds.add(legacyHash != null ? new OperatorID(legacyHash) : null);
			}
		}

		// create job vertex
		String jobVertexName = makeJobVertexName(storager.chainedHeadNodeIdsInOrder, storager.chainedNameMap);

		StreamNode startStreamNode = streamGraph.getStreamNode(startNodeId);
		if (storager.chainInputFormatMap.size() != 0 || storager.chainOutputFormatMap.size() != 0) {
			jobVertex = new MultiInputOutputFormatVertex(
				jobVertexName,
				jobVertexId,
				legacyJobVertexIds,
				chainedOperatorVertexIds,
				userDefinedChainedOperatorVertexIds);

			TaskConfig taskConfig = new TaskConfig(jobVertex.getConfiguration());
			FormatUtil.MultiFormatStub.setStubFormats(
				taskConfig,
				storager.chainInputFormatMap.size() == 0 ? null : storager.chainInputFormatMap,
				storager.chainOutputFormatMap.size() == 0 ? null : storager.chainOutputFormatMap);
		} else {
			jobVertex = new JobVertex(
					jobVertexName,
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
		}

		boolean hasNodeWithChainedMultiInputs = false;
		for (Integer nodeId : storager.chainedNodeIdsInOrder) {
			final byte[] hash = hashes.get(nodeId);
			final StreamNode node = streamGraph.getStreamNode(nodeId);
			final OperatorID operatorID = new OperatorID(hash);
			final OperatorDescriptor operatorDescriptor = new OperatorDescriptor(node.getOperatorName(), operatorID);

			int inEdgesNumInChain = 0;
			for (StreamEdge streamEdge : node.getInEdges()) {
				final OperatorEdgeDescriptor edgeDescriptor = new OperatorEdgeDescriptor(
					new OperatorID(hashes.get(streamEdge.getSourceId())),
					operatorID,
					streamEdge.getTypeNumber(),
					streamEdge.getPartitioner() == null ? "null" : streamEdge.getPartitioner().toString());
				operatorDescriptor.addInput(edgeDescriptor);

				if (storager.chainedConfigMap.containsKey(streamEdge.getSourceId())) {
					inEdgesNumInChain++;
				}
			}
			jobVertex.addOperatorDescriptor(operatorDescriptor);

			if (!hasNodeWithChainedMultiInputs && inEdgesNumInChain > 1) {
				hasNodeWithChainedMultiInputs = true;
			}
		}

		// set properties of job vertex
		jobVertex.setResources(storager.chainedMinResources, storager.chainedPreferredResources);
		if (storager.chainedHeadNodeIdsInOrder.size() > 1 || hasNodeWithChainedMultiInputs) {
			jobVertex.setInvokableClass(ArbitraryInputStreamTask.class);
		} else {
			jobVertex.setInvokableClass(startStreamNode.getJobVertexClass());
		}

		int parallelism = startStreamNode.getParallelism();
		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, startNodeId);
		}

		jobVertex.setMaxParallelism(startStreamNode.getMaxParallelism());

		return jobVertex;
	}

	private void setupVertexConfig(StreamConfig anyheadNodeConfig, CreatingChainIntermediateStorager storager, Configuration config) {
		StreamTaskConfigCache configCache = storager.vertexConfigCache;

		configCache.setTimeCharacteristic(anyheadNodeConfig.getTimeCharacteristic());
		configCache.setCheckpointingEnabled(anyheadNodeConfig.isCheckpointingEnabled());
		configCache.setCheckpointMode(anyheadNodeConfig.getCheckpointMode());
		configCache.setStateBackend(anyheadNodeConfig.getStateBackend(storager.classLoader));

		configCache.setChainedNodeConfigs(storager.chainedConfigMap);
		configCache.setChainedHeadNodeIds(storager.chainedHeadNodeIdsInOrder);
		configCache.setInStreamEdgesOfChain(storager.chainInEdgesInOrder);
		configCache.setOutStreamEdgesOfChain(storager.chainOutEdgesInOrder);

		configCache.serializeTo(new StreamTaskConfig(config));
	}

	private static void setupNodeConfig(Integer nodeId,
		List<StreamEdge> nonChainableInputs,
		List<StreamEdge> chainableOutputs,
		List<StreamEdge> nonChainableOutputs,
		StreamGraph streamGraph,
		StreamConfig config) {

		StreamNode vertex = streamGraph.getStreamNode(nodeId);

		config.setVertexID(nodeId);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperator(vertex.getOperator());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfInputs(nonChainableInputs.size());
		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		final CheckpointConfig ceckpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(ceckpointCfg.isCheckpointingEnabled());
		if (ceckpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(ceckpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(nodeId));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(nodeId));
		}

		Configuration customConfiguration = vertex.getCustomConfiguration();
		if (customConfiguration.keySet().size() > 0) {
			config.setCustomConfiguration(customConfiguration);
		}
	}

	private static String makeChainedName(String currentOperatorName, List<StreamEdge> chainedOutputs, Map<Integer, String> chainedNames) {
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return currentOperatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return currentOperatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return currentOperatorName;
		}
	}

	private static String makeJobVertexName(List<Integer> sortedHeadNodeIds, Map<Integer, String> chainedNames) {
		StringBuilder nameBuffer = new StringBuilder();

		int chainedHeadNodeCount = sortedHeadNodeIds.size();
		if (chainedHeadNodeCount > 1) {
			nameBuffer.append("[");
		}
		for (int i = 0; i < chainedHeadNodeCount; i++) {
			if (i > 0) {
				nameBuffer.append(", ");
			}
			nameBuffer.append(chainedNames.get(sortedHeadNodeIds.get(i)));
		}
		if (chainedHeadNodeCount > 1) {
			nameBuffer.append("]");
		}
		return nameBuffer.toString();
	}

	private static ResultPartitionType getEdgeResultPartitionType(DataExchangeMode dataExchangeMode, ExecutionMode executionMode) {
		switch (dataExchangeMode) {
			case AUTO:
				switch (executionMode) {
					case PIPELINED:
						return ResultPartitionType.PIPELINED;
					case BATCH:
						return ResultPartitionType.BLOCKING;
					default:
						throw new UnsupportedOperationException("Unknown execution mode " +	executionMode + ".");
				}
			case PIPELINED:
				return ResultPartitionType.PIPELINED;
			case BATCH:
				return ResultPartitionType.BLOCKING;
			case PIPELINE_WITH_BATCH_FALLBACK:
				throw new UnsupportedOperationException("Data exchange mode " +
					dataExchangeMode + " is not supported.");
			default:
				throw new UnsupportedOperationException("Unknown data exchange mode " + dataExchangeMode + ".");
		}
	}

	private void connectEdges() {
		for (StreamEdge edge : transitiveOutEdges) {
			JobVertex upstreamVertex = nodeToJobVertexMap.get(edge.getSourceId());
			JobVertex downstreamVertex = nodeToJobVertexMap.get(edge.getTargetId());

			if (upstreamVertex.getID().equals(downstreamVertex.getID())) {
				throw new RuntimeException("The job graph is cyclic.");
			}

			StreamPartitioner<?> partitioner = edge.getPartitioner();
			IntermediateDataSetID dataSetID = new IntermediateDataSetID(edge.getEdgeID());
			ExecutionMode executionMode = streamGraph.getExecutionConfig().getExecutionMode();
			JobEdge jobEdge;
			if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
				jobEdge = downstreamVertex.connectDataSetAsInput(
					upstreamVertex,
					dataSetID,
					DistributionPattern.POINTWISE,
					getEdgeResultPartitionType(edge.getDataExchangeMode(), executionMode));
			} else {
				jobEdge = downstreamVertex.connectDataSetAsInput(
					upstreamVertex,
					dataSetID,
					DistributionPattern.ALL_TO_ALL,
					getEdgeResultPartitionType(edge.getDataExchangeMode(), executionMode));
			}
			// set strategy name so that web interface can show it.
			jobEdge.setShipStrategyName(partitioner.toString());

			if (LOG.isDebugEnabled()) {
				LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					edge.getSourceId(), edge.getTargetId());
			}
		}
	}

	private void setSlotSharing() {

		Map<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();

		for (Integer startHeadNodeId : chainedNodeIdsMap.keySet()) {
			JobVertex vertex = nodeToJobVertexMap.get(startHeadNodeId);
			String slotSharingGroup = streamGraph.getStreamNode(startHeadNodeId).getSlotSharingGroup();
			if (slotSharingGroup == null) {
				continue;
			}

			SlotSharingGroup group = slotSharingGroups.get(slotSharingGroup);
			if (group == null) {
				group = new SlotSharingGroup();
				slotSharingGroups.put(slotSharingGroup, group);
			}
			vertex.setSlotSharingGroup(group);
		}

		for (Tuple2<StreamNode, StreamNode> pair : streamGraph.getIterationSourceSinkPairs()) {

			CoLocationGroup ccg = new CoLocationGroup();

			JobVertex source = nodeToJobVertexMap.get(pair.f0.getId());
			JobVertex sink = nodeToJobVertexMap.get(pair.f1.getId());

			ccg.addVertex(source);
			ccg.addVertex(sink);
			source.updateCoLocationGroup(ccg);
			sink.updateCoLocationGroup(ccg);
		}

	}

	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval > 0) {

			ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
			// propagate the expected behaviour for checkpoint errors to task.
			executionConfig.setFailTaskOnCheckpointError(cfg.isFailOnCheckpointingErrors());

			// check if a restart strategy has been set, if not then set the FixedDelayRestartStrategy
			if (executionConfig.getRestartStrategy() == null) {
				// if the user enabled checkpointing, the default number of exec retries is infinite.
				executionConfig.setRestartStrategy(
					RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, DEFAULT_RESTART_DELAY));
			}
		} else {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(chainedNodeIdsMap.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(chainedNodeIdsMap.size());

		for (Integer startHeadNodeId : chainedNodeIdsMap.keySet()) {
			JobVertex vertex = nodeToJobVertexMap.get(startHeadNodeId);
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			StreamOperator<?> op = node.getOperator();
			if (op instanceof AbstractUdfStreamOperator) {
				Function f = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}

	/**
	 * Temporary storage for creating chains.
	 */
	private static class CreatingChainIntermediateStorager {
		// ------------------------------------------------------------------------
		//  Global variables for creating all chains
		// ------------------------------------------------------------------------

		final Set<Integer> allBuiltNodes;
		final ClassLoader classLoader;

		// ------------------------------------------------------------------------
		//  Temporary variables for creating one chain
		// ------------------------------------------------------------------------

		final Map<Integer, StreamConfig> chainedConfigMap;
		final List<Integer> chainedHeadNodeIdsInOrder;
		final List<StreamEdge> chainInEdgesInOrder;
		final List<StreamEdge> chainOutEdgesInOrder;

		final Map<OperatorID, InputFormat> chainInputFormatMap;
		final Map<OperatorID, OutputFormat> chainOutputFormatMap;

		final Map<Integer, String> chainedNameMap;
		ResourceSpec chainedMinResources;
		ResourceSpec chainedPreferredResources;

		JobVertex createdVertex;

		final StreamTaskConfigCache vertexConfigCache;
		final List<Integer> chainedNodeIdsInOrder;

		CreatingChainIntermediateStorager() {
			allBuiltNodes = new HashSet<>();
			this.classLoader = Thread.currentThread().getContextClassLoader();

			this.chainedConfigMap = new HashMap<>();
			this.chainedHeadNodeIdsInOrder = new ArrayList<>();
			this.chainInEdgesInOrder = new ArrayList<>();
			this.chainOutEdgesInOrder = new ArrayList<>();

			this.chainInputFormatMap = new HashMap<>();
			this.chainOutputFormatMap = new HashMap<>();

			this.chainedNameMap = new HashMap<>();

			this.vertexConfigCache = new StreamTaskConfigCache(classLoader);
			this.chainedNodeIdsInOrder = new ArrayList<>();
		}

		void resetForNewChain() {
			this.chainedConfigMap.clear();
			this.chainedHeadNodeIdsInOrder.clear();
			this.chainInEdgesInOrder.clear();
			this.chainOutEdgesInOrder.clear();

			this.chainInputFormatMap.clear();
			this.chainOutputFormatMap.clear();

			this.chainedNameMap.clear();
			this.chainedMinResources = null;
			this.chainedPreferredResources = null;

			this.createdVertex = null;

			this.vertexConfigCache.clear();
			this.chainedNodeIdsInOrder.clear();
		}
	}

	/**
	 * The representation of a {@link StreamNode} to set chaining.
	 */
	static class ChainingStreamNode {

		private final Integer nodeId;
		private final int inEdgeCnt;

		private final int topologicalOrder;
		private final int layerNumber;

		private int depthFirstNumber;
		private Map<ReadPriority, Set<Integer>> downPriorityMap;
		private Map<Integer, ReadPriority> readPriorityMap;
		private Map<ReadPriority, Integer> priorityInEdgeNumMap;
		private Integer coarsenedId;

		private Set<Integer> chainableToSet;

		private Boolean allowMultiHeadChaining;

		ChainingStreamNode(Integer nodeId, int inEdgeCnt, int topologicalOrder, int layerNumber) {
			this.nodeId = nodeId;
			this.inEdgeCnt = inEdgeCnt;
			this.topologicalOrder = topologicalOrder;
			this.layerNumber = layerNumber;

			this.downPriorityMap = new HashMap<>();
			this.readPriorityMap = new HashMap<>();
			this.priorityInEdgeNumMap = new HashMap<>();
		}

		int getNodeId() {
			return nodeId;
		}

		int getTopologicalOrder() {
			return topologicalOrder;
		}

		int getLayerNumber() {
			return layerNumber;
		}

		int getDepthFirstNumber() {
			return depthFirstNumber;
		}

		void setDepthFirstNumber(int depthFirstNumber) {
			this.depthFirstNumber = depthFirstNumber;
		}

		ReadPriority getTransitivePriority() {
			final ReadPriority priority;

			if (isDownPriorityConflicting()) {
				priority = ReadPriority.DYNAMIC;
			} else if (downPriorityMap.size() == 1) {
				priority = downPriorityMap.keySet().iterator().next();
			} else if (downPriorityMap.size() == 0) {
				priority = null;
			} else {
				// should not arrive here
				throw new IllegalStateException("This is an internal error.");
			}

			return priority;
		}

		boolean isDownPriorityConflicting() {
			return (downPriorityMap.size() > 1 ||
					downPriorityMap.getOrDefault(ReadPriority.DYNAMIC, Collections.EMPTY_SET).size() > 1);
		}

		Set<Integer> getDownPriorityNodes(ReadPriority priority) {
			return this.downPriorityMap.get(priority);
		}

		void setDownPriority(Integer downstreamNodeId, ReadPriority priority) {
			checkState(priority != null);
			downPriorityMap.computeIfAbsent(priority, k -> new HashSet<>())
					.add(downstreamNodeId);
		}

		ReadPriority getReadPriority(Integer upstreamNodeId) {
			return readPriorityMap.get(upstreamNodeId);
		}

		boolean isReadPriorityConflicting() {
			return (priorityInEdgeNumMap.size() > 1 ||
					priorityInEdgeNumMap.getOrDefault(ReadPriority.DYNAMIC, 0) > 1);
		}

		void setReadPriority(Integer upstreamNodeId, ReadPriority priority) {
			checkState(priority != null);

			readPriorityMap.put(upstreamNodeId, priority);
			priorityInEdgeNumMap.put(priority, priorityInEdgeNumMap.getOrDefault(priority, 0) + 1);
		}

		Integer getCoarsenedId() {
			return coarsenedId;
		}

		void setCoarsenedId(Integer coarsenedId) {
			this.coarsenedId = coarsenedId;
		}

		boolean isChainHeadNode() {
			return inEdgeCnt == 0 || inEdgeCnt > (chainableToSet == null ? 0 : chainableToSet.size());
		}

		boolean isChainTo(Integer upstreamNodeId) {
			return chainableToSet != null && chainableToSet.contains(upstreamNodeId);
		}

		void setAllowMultiHeadChaining(Boolean allowMultiHeadChaining) {
			checkState(this.allowMultiHeadChaining == null || this.allowMultiHeadChaining == allowMultiHeadChaining,
				"The flag allowMultiHeadChaining can not be changed (nodeId: %s).", nodeId);

			this.allowMultiHeadChaining = allowMultiHeadChaining;
		}

		void chainTo(ChainingStreamNode upstreamChainingNode,
			StreamEdge edge,
			StreamNode sourceNode,
			StreamNode targetNode,
			boolean isMultiHeadChainMode,
			boolean isEagerChainingEnabled,
			ExecutionMode executionMode) {

			final boolean isChainable;
			if (upstreamChainingNode.allowMultiHeadChaining && isMultiHeadChainMode) {
				isChainable = isChainableOnMultiHeadMode(edge, sourceNode, targetNode, isEagerChainingEnabled, executionMode);
			} else {
				isChainable = isChainable(edge, sourceNode, targetNode, isEagerChainingEnabled, executionMode);
			}

			if (isChainable) {
				addChainableToNode(upstreamChainingNode.nodeId);

				setAllowMultiHeadChaining(upstreamChainingNode.allowMultiHeadChaining);
			} else {
				setAllowMultiHeadChaining(Boolean.TRUE);
			}
		}

		private void addChainableToNode(Integer upstreamNodeId) {
			if (chainableToSet == null) {
				chainableToSet = new HashSet<>();
			}
			chainableToSet.add(upstreamNodeId);
		}

		void removeChainableToNode(Integer upstreamNodeId) {
			if (chainableToSet != null) {
				chainableToSet.remove(upstreamNodeId);
			}
		}

		private boolean isChainable(StreamEdge edge,
			StreamNode upstreamNode,
			StreamNode downStreamNode,
			boolean chainEagerlyEnabled,
			ExecutionMode executionMode) {

			return downStreamNode.getInEdges().size() == 1
				&& isChainableOnMultiHeadMode(edge, upstreamNode, downStreamNode, chainEagerlyEnabled, executionMode);
		}

		private boolean isChainableOnMultiHeadMode(StreamEdge edge,
			StreamNode upstreamNode,
			StreamNode downStreamNode,
			boolean chainEagerlyEnabled,
			ExecutionMode executionMode) {

			StreamOperator<?> downstreamOperator = downStreamNode.getOperator();
			StreamOperator<?> upstreamOperator = upstreamNode.getOperator();

			return downstreamOperator != null
				&& upstreamOperator != null
				&& downStreamNode.isSameSlotSharingGroup(upstreamNode)
				&& downstreamOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (upstreamOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
				upstreamOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner ||
					(downStreamNode.getParallelism() == 1 && chainEagerlyEnabled))
				&& downStreamNode.getParallelism() == upstreamNode.getParallelism()
				&& edge.getDataExchangeMode() != DataExchangeMode.BATCH;
		}
	}

	/**
	 * The representation of a set of mergeable {@link StreamNode}s. In a coarsened node
	 * which includes multiple {@link StreamNode}s, for any {@link StreamNode}, there is
	 * at least one such {@link StreamNode} with one {@link StreamEdge} between them. And
	 * the directed Graph formed by all coarsened nodes must be acyclic.
	 */
	static class CoarsenedNode {
		private final Integer id;

		private final Set<Integer> originalNodes = new HashSet<>();
		private final Map<Integer, Boolean> predecessorsNodes = new HashMap<>();
		private final Set<Integer> sucessorNodes = new HashSet<>();

		public CoarsenedNode(StreamNode streamNode, ChainingStreamNode chainingNode) {
			this.id = streamNode.getId();
			this.originalNodes.add(streamNode.getId());

			for (StreamEdge inEdge : streamNode.getInEdges()) {
				Integer sourceId = inEdge.getSourceId();
				this.predecessorsNodes.put(sourceId, chainingNode.isChainTo(sourceId));
			}

			for (StreamEdge outEdge : streamNode.getOutEdges()) {
				this.sucessorNodes.add(outEdge.getTargetId());
			}
		}

		public Integer getId() {
			return this.id;
		}

		public Set<Integer> getOriginalNodes() {
			return this.originalNodes;
		}

		public Map<Integer, Boolean> getPredecessorsNodes() {
			return this.predecessorsNodes;
		}

		public Set<Integer> getSucessorNodes() {
			return this.sucessorNodes;
		}

		public void merge(CoarsenedNode other, Map<Integer, CoarsenedNode> coarsenedNodeMap) {
			originalNodes.addAll(other.originalNodes);

			for (Map.Entry<Integer, Boolean> entry : other.predecessorsNodes.entrySet()) {
				Integer otherPredNodeId = entry.getKey();
				predecessorsNodes.put(otherPredNodeId,
						other.predecessorsNodes.get(otherPredNodeId) && predecessorsNodes.getOrDefault(otherPredNodeId, Boolean.TRUE));

				Set<Integer> updateNodes = coarsenedNodeMap.get(otherPredNodeId).sucessorNodes;
				updateNodes.remove(other.id);
				updateNodes.add(this.id);
			}

			for (Integer otherSuccNodeId : other.sucessorNodes) {
				sucessorNodes.add(otherSuccNodeId);

				Map<Integer, Boolean> updateNodes = coarsenedNodeMap.get(otherSuccNodeId).predecessorsNodes;
				updateNodes.put(this.id,
						updateNodes.get(other.id) && updateNodes.getOrDefault(this.id, Boolean.TRUE));
				updateNodes.remove(other.id);
			}

			predecessorsNodes.remove(this.id);
			sucessorNodes.remove(this.id);
			predecessorsNodes.remove(other.id);
			sucessorNodes.remove(other.id);
		}
	}

	/**
	 * Generates the sequence of numbers from zero.
	 */
	static class SequenceGenerator {

		private int sequence = 0;

		public int get() {
			return sequence++;
		}

		public int last() {
			return sequence;
		}
	}
}
