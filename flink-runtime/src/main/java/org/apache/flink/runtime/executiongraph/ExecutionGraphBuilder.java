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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.SimpleCheckpointStatsTracker;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link JobGraph}.
 */
public class ExecutionGraphBuilder {
	/**
	 * Builds the ExecutionGraph from the JobGraph.
	 * If a prior execution graph exists, the JobGraph will be attached. If no prior execution
	 * graph exists, then the JobGraph will become attach to a new emoty execution graph.
	 */
	public static ExecutionGraph buildGraph(
			@Nullable ExecutionGraph prior,
			JobGraph jobGraph,
			Configuration jobManagerConfig,
			Executor futureExecutor,
			Executor ioExecutor,
			ClassLoader classLoader,
			CheckpointRecoveryFactory recoveryFactory,
			Time timeout,
			RestartStrategy restartStrategy,
			MetricGroup metrics,
			int parallelismForAutoMax,
			Logger log)
		throws JobExecutionException, JobException
	{
		checkNotNull(jobGraph, "job graph cannot be null");

		final String jobName = jobGraph.getName();
		final JobID jobId = jobGraph.getJobID();

		// create a new execution graph, if none exists so far
		final ExecutionGraph executionGraph;

		try {
			executionGraph = (prior != null) ? prior :
					new ExecutionGraph(
						futureExecutor,
						ioExecutor,
						jobId,
						jobName,
						jobGraph.getJobConfiguration(),
						jobGraph.getSerializedExecutionConfig(),
						timeout,
						restartStrategy,
						jobGraph.getUserJarBlobKeys(),
						jobGraph.getClasspaths(),
						classLoader,
						metrics);
		} catch (IOException e) {
			throw new JobException("Could not create the execution graph.", e);
		}

		// set the basic properties

		executionGraph.setScheduleMode(jobGraph.getScheduleMode());
		executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling());

		try {
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
		}
		catch (Throwable t) {
			log.warn("Cannot create JSON plan for job", t);
			// give the graph an empty plan
			executionGraph.setJsonPlan("{}");
		}

		// initialize the vertices that have a master initialization hook
		// file output formats create directories here, input formats create splits

		final long initMasterStart = System.nanoTime();
		log.info("Running initialization on master for job {} ({}).", jobName, jobId);

		for (JobVertex vertex : jobGraph.getVertices()) {
			String executableClass = vertex.getInvokableClassName();
			if (executableClass == null || executableClass.isEmpty()) {
				throw new JobSubmissionException(jobId,
						"The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
			}

			if (vertex.getParallelism() == ExecutionConfig.PARALLELISM_AUTO_MAX) {
				vertex.setParallelism(parallelismForAutoMax);
			}

			try {
				vertex.initializeOnMaster(classLoader);
			}
			catch (Throwable t) {
					throw new JobExecutionException(jobId,
							"Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
			}
		}

		log.info("Successfully ran initialization on master in {} ms.",
				(System.nanoTime() - initMasterStart) / 1_000_000);

		// topologically sort the job vertices and attach the graph to the existing one
		List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
		if (log.isDebugEnabled()) {
			log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
		}
		executionGraph.attachJobGraph(sortedTopology);

		if (log.isDebugEnabled()) {
			log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
		}

		// configure the state checkpointing
		JobSnapshottingSettings snapshotSettings = jobGraph.getSnapshotSettings();
		if (snapshotSettings != null) {
			List<ExecutionJobVertex> triggerVertices = 
					idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

			List<ExecutionJobVertex> ackVertices =
					idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

			List<ExecutionJobVertex> confirmVertices =
					idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

			CompletedCheckpointStore completedCheckpoints;
			CheckpointIDCounter checkpointIdCounter;
			try {
				completedCheckpoints = recoveryFactory.createCheckpointStore(jobId, classLoader);
				checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
			}
			catch (Exception e) {
				throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
			}

			// Checkpoint stats tracker
			boolean isStatsDisabled = jobManagerConfig.getBoolean(
					ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_DISABLE,
					ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_DISABLE);

			CheckpointStatsTracker checkpointStatsTracker;
			if (isStatsDisabled) {
				checkpointStatsTracker = new DisabledCheckpointStatsTracker();
			}
			else {
				int historySize = jobManagerConfig.getInteger(
						ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE,
						ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE);

				checkpointStatsTracker = new SimpleCheckpointStatsTracker(historySize, ackVertices, metrics);
			}

			/** The default directory for externalized checkpoints. */
			String externalizedCheckpointsDir = jobManagerConfig.getString(
					ConfigConstants.CHECKPOINTS_DIRECTORY_KEY, null);

			executionGraph.enableSnapshotCheckpointing(
					snapshotSettings.getCheckpointInterval(),
					snapshotSettings.getCheckpointTimeout(),
					snapshotSettings.getMinPauseBetweenCheckpoints(),
					snapshotSettings.getMaxConcurrentCheckpoints(),
					snapshotSettings.getExternalizedCheckpointSettings(),
					triggerVertices,
					ackVertices,
					confirmVertices,
					checkpointIdCounter,
					completedCheckpoints,
					externalizedCheckpointsDir,
					checkpointStatsTracker);
		}

		return executionGraph;
	}

	private static List<ExecutionJobVertex> idToVertex(
			List<JobVertexID> jobVertices, ExecutionGraph executionGraph) throws IllegalArgumentException {

		List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

		for (JobVertexID id : jobVertices) {
			ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
			if (vertex != null) {
				result.add(vertex);
			} else {
				throw new IllegalArgumentException(
						"The snapshot checkpointing settings refer to non-existent vertex " + id);
			} 
		}

		return result;
	}

	// ------------------------------------------------------------------------

	/** This class is not supposed to be instantiated */
	private ExecutionGraphBuilder() {}
}
