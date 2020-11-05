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

package org.apache.flink.state.api.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.memory.MemoryManager.DEFAULT_PAGE_SIZE;

/**
 * A minimally implemented {@link Environment} that provides the functionality required to run the
 * {@code state-processor-api}.
 */
@Internal
public class SavepointEnvironment implements Environment {
	private static final String ERROR_MSG = "This method should never be called";

	private final JobID jobID;

	private final JobVertexID vertexID;

	private final ExecutionAttemptID attemptID;

	private final RuntimeContext ctx;

	private final Configuration configuration;

	private final int maxParallelism;

	private final int indexOfSubtask;

	private final TaskKvStateRegistry registry;

	private final TaskStateManager taskStateManager;

	private final IOManager ioManager;

	private final MemoryManager memoryManager;

	private final AccumulatorRegistry accumulatorRegistry;

	private final UserCodeClassLoader userCodeClassLoader;

	private SavepointEnvironment(RuntimeContext ctx, Configuration configuration, int maxParallelism, int indexOfSubtask, PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState) {
		this.jobID = new JobID();
		this.vertexID = new JobVertexID();
		this.attemptID = new ExecutionAttemptID();
		this.ctx = Preconditions.checkNotNull(ctx);
		this.configuration = Preconditions.checkNotNull(configuration);

		Preconditions.checkArgument(maxParallelism > 0 && indexOfSubtask < maxParallelism);
		this.maxParallelism = maxParallelism;
		this.indexOfSubtask = indexOfSubtask;

		this.registry = new KvStateRegistry().createTaskRegistry(jobID, vertexID);
		this.taskStateManager = new SavepointTaskStateManager(prioritizedOperatorSubtaskState);
		this.ioManager = new IOManagerAsync(ConfigurationUtils.parseTempDirectories(configuration));
		this.memoryManager = MemoryManager.create(64 * 1024 * 1024, DEFAULT_PAGE_SIZE);
		this.accumulatorRegistry = new AccumulatorRegistry(jobID, attemptID);

		this.userCodeClassLoader = UserCodeClassLoaderRuntimeContextAdapter.from(ctx);
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return ctx.getExecutionConfig();
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public JobVertexID getJobVertexId() {
		return vertexID;
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return attemptID;
	}

	@Override
	public Configuration getTaskConfiguration() {
		return configuration;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return new SavepointTaskManagerRuntimeInfo(getIOManager());
	}

	@Override
	public TaskMetricGroup getMetricGroup() {
		return UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
	}

	@Override
	public Configuration getJobConfiguration() {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public TaskInfo getTaskInfo() {
		return new TaskInfo(
			ctx.getTaskName(),
			maxParallelism,
			indexOfSubtask,
			ctx.getNumberOfParallelSubtasks(),
			ctx.getAttemptNumber());
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public IOManager getIOManager() {
		return ioManager;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return memoryManager;
	}

	@Override
	public UserCodeClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return Collections.emptyMap();
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public TaskStateManager getTaskStateManager() {
		return taskStateManager;
	}

	@Override
	public GlobalAggregateManager getGlobalAggregateManager() {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public ExternalResourceInfoProvider getExternalResourceInfoProvider() {
		// We will still construct a StreamingRuntimeContext from this SavepointEnvironment at the moment. So, throwing
		// Exception here would cause issue. When there is a bounded DataStream API, this class would be removed.
		return ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;
	}

	@Override
	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	@Override
	public TaskKvStateRegistry getTaskKvStateRegistry() {
		return registry;
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public void declineCheckpoint(long checkpointId, Throwable cause) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
		return new NoOpTaskOperatorEventGateway();
	}

	@Override
	public void failExternally(Throwable cause) {
		ExceptionUtils.rethrow(cause);
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		return new ResultPartitionWriter[0];
	}

	@Override
	public IndexedInputGate getInputGate(int index) {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	@Override
	public IndexedInputGate[] getAllInputGates() {
		return new IndexedInputGate[0];
	}

	@Override
	public TaskEventDispatcher getTaskEventDispatcher() {
		throw new UnsupportedOperationException(ERROR_MSG);
	}

	/**
	 * {@link SavepointEnvironment} builder.
	 */
	public static class Builder {
		private RuntimeContext ctx;

		private Configuration configuration;

		private int maxParallelism;

		private int indexOfSubtask;

		private PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState;

		public Builder(RuntimeContext ctx, int maxParallelism) {
			this.ctx = Preconditions.checkNotNull(ctx);

			Preconditions.checkArgument(maxParallelism > 0);
			this.maxParallelism = maxParallelism;

			this.prioritizedOperatorSubtaskState = PrioritizedOperatorSubtaskState.emptyNotRestored();
			this.configuration = new Configuration();
			this.indexOfSubtask = ctx.getIndexOfThisSubtask();
		}

		public Builder setSubtaskIndex(int indexOfSubtask) {
			this.indexOfSubtask = indexOfSubtask;
			return this;
		}

		public Builder setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}

		public Builder setPrioritizedOperatorSubtaskState(PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState) {
			this.prioritizedOperatorSubtaskState = prioritizedOperatorSubtaskState;
			return this;
		}

		public SavepointEnvironment build() {
			return new SavepointEnvironment(
				ctx,
				configuration,
				maxParallelism,
				indexOfSubtask,
				prioritizedOperatorSubtaskState);
		}
	}

	private static final class UserCodeClassLoaderRuntimeContextAdapter implements UserCodeClassLoader {

		private final RuntimeContext runtimeContext;

		private UserCodeClassLoaderRuntimeContextAdapter(RuntimeContext runtimeContext) {
			this.runtimeContext = runtimeContext;
		}

		@Override
		public ClassLoader asClassLoader() {
			return runtimeContext.getUserCodeClassLoader();
		}

		@Override
		public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
			runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(releaseHookName, releaseHook);
		}

		private static UserCodeClassLoaderRuntimeContextAdapter from(RuntimeContext runtimeContext) {
			return new UserCodeClassLoaderRuntimeContextAdapter(runtimeContext);
		}
	}

	// ------------------------------------------------------------------------
	//  mocks / stand-ins
	// ------------------------------------------------------------------------

	private static final class NoOpTaskOperatorEventGateway implements TaskOperatorEventGateway {
		@Override
		public void sendOperatorEventToCoordinator(OperatorID operator, SerializedValue<OperatorEvent> event) {}
	}
}

