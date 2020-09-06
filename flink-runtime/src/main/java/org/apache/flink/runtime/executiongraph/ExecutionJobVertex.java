/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.types.Either;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@code ExecutionJobVertex} is part of the {@link ExecutionGraph}, and the peer
 * to the {@link JobVertex}.
 *
 * <p>The {@code ExecutionJobVertex} corresponds to a parallelized operation. It
 * contains an {@link ExecutionVertex} for each parallel instance of that operation.
 */
public class ExecutionJobVertex implements AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

	/** Use the same log for all ExecutionGraph classes. */
	private static final Logger LOG = ExecutionGraph.LOG;

	public static final int VALUE_NOT_SET = -1;

	private final Object stateMonitor = new Object();

	private final ExecutionGraph graph;

	private final JobVertex jobVertex;

	private final ExecutionVertex[] taskVertices;

	private final IntermediateResult[] producedDataSets;

	private final List<IntermediateResult> inputs;

	private final int parallelism;

	private final SlotSharingGroup slotSharingGroup;

	private final CoLocationGroup coLocationGroup;

	private final InputSplit[] inputSplits;

	private final boolean maxParallelismConfigured;

	private int maxParallelism;

	private final ResourceProfile resourceProfile;

	/**
	 * Either store a serialized task information, which is for all sub tasks the same,
	 * or the permanent blob key of the offloaded task information BLOB containing
	 * the serialized task information.
	 */
	private Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey = null;

	private final Collection<OperatorCoordinatorHolder> operatorCoordinators;

	private InputSplitAssigner splitAssigner;

	/**
	 * Convenience constructor for testing.
	 */
	@VisibleForTesting
	ExecutionJobVertex(
			ExecutionGraph graph,
			JobVertex jobVertex,
			int defaultParallelism,
			Time timeout) throws JobException {

		this(
			graph,
			jobVertex,
			defaultParallelism,
			JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue(),
			timeout,
			1L,
			System.currentTimeMillis());
	}

	public ExecutionJobVertex(
			ExecutionGraph graph,
			JobVertex jobVertex,
			int defaultParallelism,
			int maxPriorAttemptsHistoryLength,
			Time timeout,
			long initialGlobalModVersion,
			long createTimestamp) throws JobException {

		if (graph == null || jobVertex == null) {
			throw new NullPointerException();
		}

		this.graph = graph;
		this.jobVertex = jobVertex;

		int vertexParallelism = jobVertex.getParallelism();
		int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;

		final int configuredMaxParallelism = jobVertex.getMaxParallelism();

		this.maxParallelismConfigured = (VALUE_NOT_SET != configuredMaxParallelism);

		// if no max parallelism was configured by the user, we calculate and set a default
		setMaxParallelismInternal(maxParallelismConfigured ?
				configuredMaxParallelism : KeyGroupRangeAssignment.computeDefaultMaxParallelism(numTaskVertices));

		// verify that our parallelism is not higher than the maximum parallelism
		if (numTaskVertices > maxParallelism) {
			throw new JobException(
				String.format("Vertex %s's parallelism (%s) is higher than the max parallelism (%s). Please lower the parallelism or increase the max parallelism.",
					jobVertex.getName(),
					numTaskVertices,
					maxParallelism));
		}

		this.parallelism = numTaskVertices;
		this.resourceProfile = ResourceProfile.fromResourceSpec(jobVertex.getMinResources(), MemorySize.ZERO);

		this.taskVertices = new ExecutionVertex[numTaskVertices];

		this.inputs = new ArrayList<>(jobVertex.getInputs().size());

		// take the sharing group
		this.slotSharingGroup = jobVertex.getSlotSharingGroup();
		this.coLocationGroup = jobVertex.getCoLocationGroup();

		// setup the coLocation group
		if (coLocationGroup != null && slotSharingGroup == null) {
			throw new JobException("Vertex uses a co-location constraint without using slot sharing");
		}

		// create the intermediate results
		this.producedDataSets = new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];

		for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
			final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);

			this.producedDataSets[i] = new IntermediateResult(
					result.getId(),
					this,
					numTaskVertices,
					result.getResultType());
		}

		// create all task vertices
		for (int i = 0; i < numTaskVertices; i++) {
			ExecutionVertex vertex = new ExecutionVertex(
					this,
					i,
					producedDataSets,
					timeout,
					initialGlobalModVersion,
					createTimestamp,
					maxPriorAttemptsHistoryLength);

			this.taskVertices[i] = vertex;
		}

		// sanity check for the double referencing between intermediate result partitions and execution vertices
		for (IntermediateResult ir : this.producedDataSets) {
			if (ir.getNumberOfAssignedPartitions() != parallelism) {
				throw new RuntimeException("The intermediate result's partitions were not correctly assigned.");
			}
		}

		final List<SerializedValue<OperatorCoordinator.Provider>> coordinatorProviders = getJobVertex().getOperatorCoordinators();
		if (coordinatorProviders.isEmpty()) {
			this.operatorCoordinators = Collections.emptyList();
		} else {
			final ArrayList<OperatorCoordinatorHolder> coordinators = new ArrayList<>(coordinatorProviders.size());
			try {
				for (final SerializedValue<OperatorCoordinator.Provider> provider : coordinatorProviders) {
					coordinators.add(OperatorCoordinatorHolder.create(provider, this, graph.getUserClassLoader()));
				}
			} catch (Exception | LinkageError e) {
				IOUtils.closeAllQuietly(coordinators);
				throw new JobException("Cannot instantiate the coordinator for operator " + getName(), e);
			}
			this.operatorCoordinators = Collections.unmodifiableList(coordinators);
		}

		// set up the input splits, if the vertex has any
		try {
			@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();

			if (splitSource != null) {
				Thread currentThread = Thread.currentThread();
				ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
				currentThread.setContextClassLoader(graph.getUserClassLoader());
				try {
					inputSplits = splitSource.createInputSplits(numTaskVertices);

					if (inputSplits != null) {
						splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
					}
				} finally {
					currentThread.setContextClassLoader(oldContextClassLoader);
				}
			}
			else {
				inputSplits = null;
			}
		}
		catch (Throwable t) {
			throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
		}
	}

	/**
	 * Returns a list containing the ID pairs of all operators contained in this execution job vertex.
	 *
	 * @return list containing the ID pairs of all contained operators
	 */
	public List<OperatorIDPair> getOperatorIDs() {
		return jobVertex.getOperatorIDs();
	}

	public void setMaxParallelism(int maxParallelismDerived) {

		Preconditions.checkState(!maxParallelismConfigured,
				"Attempt to override a configured max parallelism. Configured: " + this.maxParallelism
						+ ", argument: " + maxParallelismDerived);

		setMaxParallelismInternal(maxParallelismDerived);
	}

	private void setMaxParallelismInternal(int maxParallelism) {
		if (maxParallelism == ExecutionConfig.PARALLELISM_AUTO_MAX) {
			maxParallelism = KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;
		}

		Preconditions.checkArgument(maxParallelism > 0
						&& maxParallelism <= KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM,
				"Overriding max parallelism is not in valid bounds (1..%s), found: %s",
				KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM, maxParallelism);

		this.maxParallelism = maxParallelism;
	}

	public ExecutionGraph getGraph() {
		return graph;
	}

	public JobVertex getJobVertex() {
		return jobVertex;
	}

	@Override
	public String getName() {
		return getJobVertex().getName();
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	@Override
	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public boolean isMaxParallelismConfigured() {
		return maxParallelismConfigured;
	}

	public JobID getJobId() {
		return graph.getJobID();
	}

	@Override
	public JobVertexID getJobVertexId() {
		return jobVertex.getID();
	}

	@Override
	public ExecutionVertex[] getTaskVertices() {
		return taskVertices;
	}

	public IntermediateResult[] getProducedDataSets() {
		return producedDataSets;
	}

	public InputSplitAssigner getSplitAssigner() {
		return splitAssigner;
	}

	@Nullable
	public SlotSharingGroup getSlotSharingGroup() {
		return slotSharingGroup;
	}

	public CoLocationGroup getCoLocationGroup() {
		return coLocationGroup;
	}

	public List<IntermediateResult> getInputs() {
		return inputs;
	}

	public InputDependencyConstraint getInputDependencyConstraint() {
		return getJobVertex().getInputDependencyConstraint();
	}

	public Collection<OperatorCoordinatorHolder> getOperatorCoordinators() {
		return operatorCoordinators;
	}

	public Either<SerializedValue<TaskInformation>, PermanentBlobKey> getTaskInformationOrBlobKey() throws IOException {
		// only one thread should offload the task information, so let's also let only one thread
		// serialize the task information!
		synchronized (stateMonitor) {
			if (taskInformationOrBlobKey == null) {
				final BlobWriter blobWriter = graph.getBlobWriter();

				final TaskInformation taskInformation = new TaskInformation(
					jobVertex.getID(),
					jobVertex.getName(),
					parallelism,
					maxParallelism,
					jobVertex.getInvokableClassName(),
					jobVertex.getConfiguration());

				taskInformationOrBlobKey = BlobWriter.serializeAndTryOffload(
					taskInformation,
					getJobId(),
					blobWriter);
			}

			return taskInformationOrBlobKey;
		}
	}

	@Override
	public ExecutionState getAggregateState() {
		int[] num = new int[ExecutionState.values().length];
		for (ExecutionVertex vertex : this.taskVertices) {
			num[vertex.getExecutionState().ordinal()]++;
		}

		return getAggregateJobVertexState(num, parallelism);
	}

	//---------------------------------------------------------------------------------------------

	public void connectToPredecessors(Map<IntermediateDataSetID, IntermediateResult> intermediateDataSets) throws JobException {

		List<JobEdge> inputs = jobVertex.getInputs();

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Connecting ExecutionJobVertex %s (%s) to %d predecessors.", jobVertex.getID(), jobVertex.getName(), inputs.size()));
		}

		for (int num = 0; num < inputs.size(); num++) {
			JobEdge edge = inputs.get(num);

			if (LOG.isDebugEnabled()) {
				if (edge.getSource() == null) {
					LOG.debug(String.format("Connecting input %d of vertex %s (%s) to intermediate result referenced via ID %s.",
							num, jobVertex.getID(), jobVertex.getName(), edge.getSourceId()));
				} else {
					LOG.debug(String.format("Connecting input %d of vertex %s (%s) to intermediate result referenced via predecessor %s (%s).",
							num, jobVertex.getID(), jobVertex.getName(), edge.getSource().getProducer().getID(), edge.getSource().getProducer().getName()));
				}
			}

			// fetch the intermediate result via ID. if it does not exist, then it either has not been created, or the order
			// in which this method is called for the job vertices is not a topological order
			IntermediateResult ires = intermediateDataSets.get(edge.getSourceId());
			if (ires == null) {
				throw new JobException("Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
						+ edge.getSourceId());
			}

			this.inputs.add(ires);

			int consumerIndex = ires.registerConsumer();

			for (int i = 0; i < parallelism; i++) {
				ExecutionVertex ev = taskVertices[i];
				ev.connectSource(num, ires, edge, consumerIndex);
			}
		}
	}

	//---------------------------------------------------------------------------------------------
	//  Actions
	//---------------------------------------------------------------------------------------------

	/**
	 * Cancels all currently running vertex executions.
	 */
	public void cancel() {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.cancel();
		}
	}

	/**
	 * Cancels all currently running vertex executions.
	 *
	 * @return A future that is complete once all tasks have canceled.
	 */
	public CompletableFuture<Void> cancelWithFuture() {
		return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::cancel));
	}

	public CompletableFuture<Void> suspend() {
		return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::suspend));
	}

	@Nonnull
	private Collection<CompletableFuture<?>> mapExecutionVertices(final Function<ExecutionVertex, CompletableFuture<?>> mapFunction) {
		return Arrays.stream(getTaskVertices())
			.map(mapFunction)
			.collect(Collectors.toList());
	}

	public void fail(Throwable t) {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.fail(t);
		}
	}

	public void resetForNewExecution(final long timestamp, final long expectedGlobalModVersion)
			throws GlobalModVersionMismatch {

		synchronized (stateMonitor) {
			// check and reset the sharing groups with scheduler hints
			for (int i = 0; i < parallelism; i++) {
				taskVertices[i].resetForNewExecution(timestamp, expectedGlobalModVersion);
			}

			// set up the input splits again
			try {
				if (this.inputSplits != null) {
					// lazy assignment
					@SuppressWarnings("unchecked")
					InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
					this.splitAssigner = splitSource.getInputSplitAssigner(this.inputSplits);
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Re-creating the input split assigner failed: " + t.getMessage(), t);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Accumulators / Metrics
	// --------------------------------------------------------------------------------------------

	public StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified() {
		Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

		for (ExecutionVertex vertex : taskVertices) {
			Map<String, Accumulator<?, ?>> next = vertex.getCurrentExecutionAttempt().getUserAccumulators();
			if (next != null) {
				AccumulatorHelper.mergeInto(userAccumulators, next);
			}
		}

		return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
	}

	// --------------------------------------------------------------------------------------------
	//  Archiving
	// --------------------------------------------------------------------------------------------

	@Override
	public ArchivedExecutionJobVertex archive() {
		return new ArchivedExecutionJobVertex(this);
	}

	// ------------------------------------------------------------------------
	//  Static Utilities
	// ------------------------------------------------------------------------

	/**
	 * A utility function that computes an "aggregated" state for the vertex.
	 *
	 * <p>This state is not used anywhere in the  coordination, but can be used for display
	 * in dashboards to as a summary for how the particular parallel operation represented by
	 * this ExecutionJobVertex is currently behaving.
	 *
	 * <p>For example, if at least one parallel task is failed, the aggregate state is failed.
	 * If not, and at least one parallel task is cancelling (or cancelled), the aggregate state
	 * is cancelling (or cancelled). If all tasks are finished, the aggregate state is finished,
	 * and so on.
	 *
	 * @param verticesPerState The number of vertices in each state (indexed by the ordinal of
	 *                         the ExecutionState values).
	 * @param parallelism The parallelism of the ExecutionJobVertex
	 *
	 * @return The aggregate state of this ExecutionJobVertex.
	 */
	public static ExecutionState getAggregateJobVertexState(int[] verticesPerState, int parallelism) {
		if (verticesPerState == null || verticesPerState.length != ExecutionState.values().length) {
			throw new IllegalArgumentException("Must provide an array as large as there are execution states.");
		}

		if (verticesPerState[ExecutionState.FAILED.ordinal()] > 0) {
			return ExecutionState.FAILED;
		}
		if (verticesPerState[ExecutionState.CANCELING.ordinal()] > 0) {
			return ExecutionState.CANCELING;
		}
		else if (verticesPerState[ExecutionState.CANCELED.ordinal()] > 0) {
			return ExecutionState.CANCELED;
		}
		else if (verticesPerState[ExecutionState.RUNNING.ordinal()] > 0) {
			return ExecutionState.RUNNING;
		}
		else if (verticesPerState[ExecutionState.FINISHED.ordinal()] > 0) {
			return verticesPerState[ExecutionState.FINISHED.ordinal()] == parallelism ?
					ExecutionState.FINISHED : ExecutionState.RUNNING;
		}
		else {
			// all else collapses under created
			return ExecutionState.CREATED;
		}
	}
}
