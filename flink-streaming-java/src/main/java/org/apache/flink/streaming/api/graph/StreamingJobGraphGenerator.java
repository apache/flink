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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	private static final int MANAGED_MEMORY_FRACTION_SCALE = 16;

	// ------------------------------------------------------------------------

	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return createJobGraph(streamGraph, null);
	}

	public static JobGraph createJobGraph(StreamGraph streamGraph, @Nullable JobID jobID) {
		return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	private final Map<Integer, JobVertex> jobVertices;
	private final JobGraph jobGraph;
	private final Collection<Integer> builtVertices;

	private final List<StreamEdge> physicalEdgesInOrder;

	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private final Map<Integer, StreamConfig> vertexConfigs;
	private final Map<Integer, String> chainedNames;

	private final Map<Integer, ResourceSpec> chainedMinResources;
	private final Map<Integer, ResourceSpec> chainedPreferredResources;

	private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph, @Nullable JobID jobID) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.chainedInputOutputFormats = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(jobID, streamGraph.getJobName());
	}

	private JobGraph createJobGraph() {
		preValidate();

		// make sure that all vertices start immediately
		jobGraph.setScheduleMode(streamGraph.getScheduleMode());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();

		setChaining(hashes, legacyHashes, chainedOperatorHashes);

		setPhysicalEdges();

		setSlotSharingAndCoLocation();

		setManagedMemoryFraction(
			Collections.unmodifiableMap(jobVertices),
			Collections.unmodifiableMap(vertexConfigs),
			Collections.unmodifiableMap(chainedConfigs),
			id -> streamGraph.getStreamNode(id).getMinResources(),
			id -> streamGraph.getStreamNode(id).getManagedMemoryWeight());

		configureCheckpointing();

		jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

		JobGraphUtils.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);

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

	@SuppressWarnings("deprecation")
	private void preValidate() {
		CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

		if (checkpointConfig.isCheckpointingEnabled()) {
			// temporarily forbid checkpointing for iterative jobs
			if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
				throw new UnsupportedOperationException(
					"Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
						+ "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
						+ "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
			}

			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				StreamOperatorFactory operatorFactory = node.getOperatorFactory();
				if (operatorFactory != null) {
					Class<?> operatorClass = operatorFactory.getStreamOperatorClass(classLoader);
					if (InputSelectable.class.isAssignableFrom(operatorClass)) {

						throw new UnsupportedOperationException(
							"Checkpointing is currently not supported for operators that implement InputSelectable:"
								+ operatorClass.getName());
					}
				}
			}
		}
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}

	private List<StreamEdge> createChain(
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		if (!builtVertices.contains(startNodeId)) {

			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				if (isChainable(outEdge, streamGraph)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);
			OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			if (currentNode.getInputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
			}

			if (currentNode.getOutputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
			}

			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)
					: new StreamConfig(new Configuration());

			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) {

				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				for (StreamEdge edge : transitiveOutEdges) {
					connect(startNodeId, edge);
				}

				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			config.setOperatorID(currentOperatorId);

			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
		return chainedInputOutputFormats
			.computeIfAbsent(startNodeId, k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;
		}
	}

	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}

	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = hashes.get(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}

		JobVertexID jobVertexId = new JobVertexID(hash);

		List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
		List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				OperatorID userDefinedOperatorID = chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
				operatorIDPairs.add(OperatorIDPair.of(new OperatorID(chainedOperator.f0), userDefinedOperatorID));
			}
		}

		if (chainedInputOutputFormats.containsKey(streamNodeId)) {
			jobVertex = new InputOutputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					operatorIDPairs);

			chainedInputOutputFormats
				.get(streamNodeId)
				.write(new TaskConfig(jobVertex.getConfiguration()));
		} else {
			jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					operatorIDPairs);
		}

		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		// TODO: inherit InputDependencyConstraint from the head operator
		jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex);

		return new StreamConfig(jobVertex.getConfiguration());
	}

	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializersIn(vertex.getTypeSerializersIn());
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

		config.setStreamOperatorFactory(vertex.getOperatorFactory());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
		config.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
		config.setCheckpointMode(getCheckpointingMode(checkpointCfg));

		for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
			config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
		}
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		vertexConfigs.put(vertexID, config);
	}

	private CheckpointingMode getCheckpointingMode(CheckpointConfig checkpointConfig) {
		CheckpointingMode checkpointingMode = checkpointConfig.getCheckpointingMode();

		checkArgument(checkpointingMode == CheckpointingMode.EXACTLY_ONCE ||
			checkpointingMode == CheckpointingMode.AT_LEAST_ONCE, "Unexpected checkpointing mode.");

		if (checkpointConfig.isCheckpointingEnabled()) {
			return checkpointingMode;
		} else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			return CheckpointingMode.AT_LEAST_ONCE;
		}
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamVertexID = edge.getTargetId();

		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();

		ResultPartitionType resultPartitionType;
		switch (edge.getShuffleMode()) {
			case PIPELINED:
				resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
				break;
			case BATCH:
				resultPartitionType = ResultPartitionType.BLOCKING;
				break;
			case UNDEFINED:
				resultPartitionType = determineResultPartitionType(partitioner);
				break;
			default:
				throw new UnsupportedOperationException("Data exchange mode " +
					edge.getShuffleMode() + " is not supported yet.");
		}

		JobEdge jobEdge;
		if (isPointwisePartitioner(partitioner)) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				resultPartitionType);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					resultPartitionType);
		}
		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamVertexID);
		}
	}

	private static boolean isPointwisePartitioner(StreamPartitioner<?> partitioner) {
		return partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner;
	}

	private ResultPartitionType determineResultPartitionType(StreamPartitioner<?> partitioner) {
		switch (streamGraph.getGlobalDataExchangeMode()) {
			case ALL_EDGES_BLOCKING:
				return ResultPartitionType.BLOCKING;
			case FORWARD_EDGES_PIPELINED:
				if (partitioner instanceof ForwardPartitioner) {
					return ResultPartitionType.PIPELINED_BOUNDED;
				} else {
					return ResultPartitionType.BLOCKING;
				}
			case POINTWISE_EDGES_PIPELINED:
				if (isPointwisePartitioner(partitioner)) {
					return ResultPartitionType.PIPELINED_BOUNDED;
				} else {
					return ResultPartitionType.BLOCKING;
				}
			case ALL_EDGES_PIPELINED:
				return ResultPartitionType.PIPELINED_BOUNDED;
			default:
				throw new RuntimeException("Unrecognized global data exchange mode " + streamGraph.getGlobalDataExchangeMode());
		}
	}

	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		return downStreamVertex.getInEdges().size() == 1
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& edge.getShuffleMode() != ShuffleMode.BATCH
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	@VisibleForTesting
	static boolean areOperatorsChainable(
			StreamNode upStreamVertex,
			StreamNode downStreamVertex,
			StreamGraph streamGraph) {
		StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();
		if (downStreamOperator == null || upStreamOperator == null) {
			return false;
		}

		if (upStreamOperator.getChainingStrategy() == ChainingStrategy.NEVER ||
			downStreamOperator.getChainingStrategy() != ChainingStrategy.ALWAYS) {
			return false;
		}

		// yielding operators cannot be chained to legacy sources
		if (downStreamOperator instanceof YieldingOperatorFactory) {
			// unfortunately the information that vertices have been chained is not preserved at this point
			return !getHeadOperator(upStreamVertex, streamGraph).isStreamSource();
		}
		return true;
	}

	/**
	 * Backtraces the head of an operator chain.
	 */
	private static StreamOperatorFactory<?> getHeadOperator(StreamNode upStreamVertex, StreamGraph streamGraph) {
		if (upStreamVertex.getInEdges().size() == 1 && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
			return getHeadOperator(streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)), streamGraph);
		}
		return upStreamVertex.getOperatorFactory();
	}

	private void setSlotSharingAndCoLocation() {
		setSlotSharing();
		setCoLocation();
	}

	private void setSlotSharing() {
		final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
		final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = buildVertexRegionSlotSharingGroups();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final JobVertex vertex = entry.getValue();
			final String slotSharingGroupKey = streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

			final SlotSharingGroup effectiveSlotSharingGroup;
			if (slotSharingGroupKey == null) {
				effectiveSlotSharingGroup = null;
			} else if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
				// fallback to the region slot sharing group by default
				effectiveSlotSharingGroup = vertexRegionSlotSharingGroups.get(vertex.getID());
			} else {
				effectiveSlotSharingGroup = specifiedSlotSharingGroups.computeIfAbsent(
					slotSharingGroupKey, k -> new SlotSharingGroup());
			}

			vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
		}
	}

	/**
	 * Maps a vertex to its region slot sharing group.
	 * If {@link StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()}
	 * returns true, all regions will be in the same slot sharing group.
	 */
	private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {
		final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
		final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();

		final boolean allRegionsInSameSlotSharingGroup = streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

		final Set<DefaultLogicalPipelinedRegion> regions = new DefaultLogicalTopology(jobGraph).getLogicalPipelinedRegions();
		for (DefaultLogicalPipelinedRegion region : regions) {
			final SlotSharingGroup regionSlotSharingGroup;
			if (allRegionsInSameSlotSharingGroup) {
				regionSlotSharingGroup = defaultSlotSharingGroup;
			} else {
				regionSlotSharingGroup = new SlotSharingGroup();
			}

			for (JobVertexID jobVertexID : region.getVertexIDs()) {
				vertexRegionSlotSharingGroups.put(jobVertexID, regionSlotSharingGroup);
			}
		}

		return vertexRegionSlotSharingGroups;
	}

	private void setCoLocation() {
		final Map<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();
			final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, k -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
				constraint.f1.addVertex(vertex);
			}
		}
	}

	private static void setManagedMemoryFraction(
			final Map<Integer, JobVertex> jobVertices,
			final Map<Integer, StreamConfig> operatorConfigs,
			final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
			final java.util.function.Function<Integer, ResourceSpec> operatorResourceRetriever,
			final java.util.function.Function<Integer, Integer> operatorManagedMemoryWeightRetriever) {

		// all slot sharing groups in this job
		final Set<SlotSharingGroup> slotSharingGroups = Collections.newSetFromMap(new IdentityHashMap<>());

		// maps a job vertex ID to its head operator ID
		final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

		// maps a job vertex ID to IDs of all operators in the vertex
		final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
			final int headOperatorId = entry.getKey();
			final JobVertex jobVertex = entry.getValue();

			final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

			checkState(jobVertexSlotSharingGroup != null, "JobVertex slot sharing group must not be null");
			slotSharingGroups.add(jobVertexSlotSharingGroup);

			vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

			final Set<Integer> operatorIds = new HashSet<>();
			operatorIds.add(headOperatorId);
			operatorIds.addAll(vertexChainedConfigs.getOrDefault(headOperatorId, Collections.emptyMap()).keySet());
			vertexOperators.put(jobVertex.getID(), operatorIds);
		}

		for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
			setManagedMemoryFractionForSlotSharingGroup(
				slotSharingGroup,
				vertexHeadOperators,
				vertexOperators,
				operatorConfigs,
				vertexChainedConfigs,
				operatorResourceRetriever,
				operatorManagedMemoryWeightRetriever);
		}
	}

	private static void setManagedMemoryFractionForSlotSharingGroup(
			final SlotSharingGroup slotSharingGroup,
			final Map<JobVertexID, Integer> vertexHeadOperators,
			final Map<JobVertexID, Set<Integer>> vertexOperators,
			final Map<Integer, StreamConfig> operatorConfigs,
			final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
			final java.util.function.Function<Integer, ResourceSpec> operatorResourceRetriever,
			final java.util.function.Function<Integer, Integer> operatorManagedMemoryWeightRetriever) {

		final int groupManagedMemoryWeight = slotSharingGroup.getJobVertexIds().stream()
			.flatMap(vid -> vertexOperators.get(vid).stream())
			.mapToInt(operatorManagedMemoryWeightRetriever::apply)
			.sum();

		for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
			for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
				final StreamConfig operatorConfig = operatorConfigs.get(operatorNodeId);
				final ResourceSpec operatorResourceSpec = operatorResourceRetriever.apply(operatorNodeId);
				final int operatorManagedMemoryWeight = operatorManagedMemoryWeightRetriever.apply(operatorNodeId);
				setManagedMemoryFractionForOperator(
					operatorResourceSpec,
					slotSharingGroup.getResourceSpec(),
					operatorManagedMemoryWeight,
					groupManagedMemoryWeight,
					operatorConfig);
			}

			// need to refresh the chained task configs because they are serialized
			final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
			final StreamConfig vertexConfig = operatorConfigs.get(headOperatorNodeId);
			vertexConfig.setTransitiveChainedTaskConfigs(vertexChainedConfigs.get(headOperatorNodeId));
		}
	}

	private static void setManagedMemoryFractionForOperator(
			final ResourceSpec operatorResourceSpec,
			final ResourceSpec groupResourceSpec,
			final int operatorManagedMemoryWeight,
			final int groupManagedMemoryWeight,
			final StreamConfig operatorConfig) {

		final double managedMemoryFraction;

		if (groupResourceSpec.equals(ResourceSpec.UNKNOWN)) {
			managedMemoryFraction = groupManagedMemoryWeight > 0
				? getFractionRoundedDown(operatorManagedMemoryWeight, groupManagedMemoryWeight)
				: 0.0;
		} else {
			final long groupManagedMemoryBytes = groupResourceSpec.getManagedMemory().getBytes();
			managedMemoryFraction = groupManagedMemoryBytes > 0
				? getFractionRoundedDown(operatorResourceSpec.getManagedMemory().getBytes(), groupManagedMemoryBytes)
				: 0.0;
		}

		operatorConfig.setManagedMemoryFraction(managedMemoryFraction);
	}

	private static double getFractionRoundedDown(final long dividend, final long divisor) {
		return BigDecimal.valueOf(dividend)
			.divide(BigDecimal.valueOf(divisor), MANAGED_MEMORY_FRACTION_SCALE, BigDecimal.ROUND_DOWN)
			.doubleValue();
	}

	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval < MINIMAL_CHECKPOINT_TIME) {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
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

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
				Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

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
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
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
				getCheckpointingMode(cfg) == CheckpointingMode.EXACTLY_ONCE,
				cfg.isUnalignedCheckpointsEnabled(),
				cfg.isPreferCheckpointForRecovery(),
				cfg.getTolerableCheckpointFailureNumber()),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
