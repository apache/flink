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
package org.apache.flink.test.state.operator.restore;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingMemoryArchivist;
import org.apache.flink.runtime.testingUtils.TestingTaskManager;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class to verify that it is possible to migrate a 1.2 savepoint to 1.3 and that the topology can be modified
 * from that point on.
 * 
 * The verification is done in 2 Steps:
 * Step 1: Migrate the job to 1.3 by submitting the same job used for the 1.2 savepoint, and create a new savepoint.
 * Step 2: Modify the job topology, and restore from the savepoint created in step 1.
 */
public abstract class AbstractOperatorRestoreTestBase {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private static ActorSystem actorSystem = null;
	private static ActorGateway jobManager = null;
	private static ActorGateway archiver = null;
	private static ActorGateway taskManager = null;

	private static final FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);

	@BeforeClass
	public static void setupCluster() throws Exception {
		FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);

		actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

		Tuple2<ActorRef, ActorRef> master = JobManager.startJobManagerActors(
			new Configuration(),
			actorSystem,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			Option.apply("jm"),
			Option.apply("arch"),
			TestingJobManager.class,
			TestingMemoryArchivist.class);

		jobManager = new AkkaActorGateway(master._1(), HighAvailabilityServices.DEFAULT_LEADER_ID);
		archiver = new AkkaActorGateway(master._2(), HighAvailabilityServices.DEFAULT_LEADER_ID);

		Configuration tmConfig = new Configuration();
		tmConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);

		ActorRef taskManagerRef = TaskManager.startTaskManagerComponentsAndActor(
			tmConfig,
			ResourceID.generate(),
			actorSystem,
			"localhost",
			Option.apply("tm"),
			Option.<LeaderRetrievalService>apply(new StandaloneLeaderRetrievalService(jobManager.path(), HighAvailabilityServices.DEFAULT_LEADER_ID)),
			true,
			TestingTaskManager.class);

		taskManager = new AkkaActorGateway(taskManagerRef, HighAvailabilityServices.DEFAULT_LEADER_ID);

		// Wait until connected
		Object msg = new TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager(jobManager.actor());
		Await.ready(taskManager.ask(msg, timeout), timeout);
	}

	@AfterClass
	public static void tearDownCluster() {
		if (actorSystem != null) {
			actorSystem.shutdown();
		}

		if (archiver != null) {
			archiver.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}

		if (jobManager != null) {
			jobManager.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}

		if (taskManager != null) {
			taskManager.actor().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}

	@Test
	public void testMigrationAndRestore() throws Throwable {
		// submit 1.2 job and create a migrated 1.3 savepoint
		String savepointPath = migrateJob();
		// restore from migrated 1.3 savepoint
		restoreJob(savepointPath);
	}

	private String migrateJob() throws Throwable {
		URL savepointResource = AbstractOperatorRestoreTestBase.class.getClassLoader().getResource("operatorstate/" + getMigrationSavepointName());
		if (savepointResource == null) {
			throw new IllegalArgumentException("Savepoint file does not exist.");
		}
		JobGraph jobToMigrate = createJobGraph(ExecutionMode.MIGRATE);
		jobToMigrate.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointResource.getFile()));

		Object msg;
		Object result;

		// Submit job graph
		msg = new JobManagerMessages.SubmitJob(jobToMigrate, ListeningBehaviour.DETACHED);
		result = Await.result(jobManager.ask(msg, timeout), timeout);

		if (result instanceof JobManagerMessages.JobResultFailure) {
			JobManagerMessages.JobResultFailure failure = (JobManagerMessages.JobResultFailure) result;
			throw new Exception(failure.cause());
		}
		Assert.assertSame(JobManagerMessages.JobSubmitSuccess.class, result.getClass());

		// Wait for all tasks to be running
		msg = new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobToMigrate.getJobID());
		Await.result(jobManager.ask(msg, timeout), timeout);

		// Trigger savepoint
		File targetDirectory = tmpFolder.newFolder();
		msg = new JobManagerMessages.CancelJobWithSavepoint(jobToMigrate.getJobID(), targetDirectory.getAbsolutePath());
		Future<Object> future = jobManager.ask(msg, timeout);
		result = Await.result(future, timeout);

		if (result instanceof JobManagerMessages.CancellationFailure) {
			JobManagerMessages.CancellationFailure failure = (JobManagerMessages.CancellationFailure) result;
			throw new Exception(failure.cause());
		}

		String savepointPath = ((JobManagerMessages.CancellationSuccess) result).savepointPath();

		// Wait until canceled
		msg = new TestingJobManagerMessages.NotifyWhenJobStatus(jobToMigrate.getJobID(), JobStatus.CANCELED);
		Await.ready(jobManager.ask(msg, timeout), timeout);

		return savepointPath;
	}

	private void restoreJob(String savepointPath) throws Exception {
		JobGraph jobToRestore = createJobGraph(ExecutionMode.RESTORE);
		jobToRestore.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, true));

		Object msg;
		Object result;

		// Submit job graph
		msg = new JobManagerMessages.SubmitJob(jobToRestore, ListeningBehaviour.DETACHED);
		result = Await.result(jobManager.ask(msg, timeout), timeout);

		if (result instanceof JobManagerMessages.JobResultFailure) {
			JobManagerMessages.JobResultFailure failure = (JobManagerMessages.JobResultFailure) result;
			throw new Exception(failure.cause());
		}
		Assert.assertSame(JobManagerMessages.JobSubmitSuccess.class, result.getClass());

		msg = new JobManagerMessages.RequestJobStatus(jobToRestore.getJobID());
		JobStatus status = ((JobManagerMessages.CurrentJobStatus) Await.result(jobManager.ask(msg, timeout), timeout)).status();
		while (!status.isTerminalState()) {
			status = ((JobManagerMessages.CurrentJobStatus) Await.result(jobManager.ask(msg, timeout), timeout)).status();
		}

		Assert.assertEquals(JobStatus.FINISHED, status);
	}

	private JobGraph createJobGraph(ExecutionMode mode) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.setStateBackend(new MemoryStateBackend());

		switch (mode) {
			case MIGRATE:
				createMigrationJob(env);
				break;
			case RESTORE:
				createRestoredJob(env);
				break;
		}

		return StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
	}

	/**
	 * Recreates the job used to create the 1.2 savepoint.
	 *
	 * @param env StreamExecutionEnvironment to use
	 */
	protected abstract void createMigrationJob(StreamExecutionEnvironment env);

	/**
	 * Creates a modified version of the job used to create the 1.2 savepoint.
	 *
	 * @param env StreamExecutionEnvironment to use
	 */
	protected abstract void createRestoredJob(StreamExecutionEnvironment env);

	/**
	 * Returns the name of the savepoint directory to use, relative to "resources/operatorstate".
	 *
	 * @return savepoint directory to use
	 */
	protected abstract String getMigrationSavepointName();
}
