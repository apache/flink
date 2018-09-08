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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.testingUtils.TestingJobManager;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Terminated;
import akka.pattern.Patterns;
import org.apache.curator.framework.CuratorFramework;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the ZooKeeper HA service and {@link JobManager} interaction.
 */
public class ZooKeeperHAJobManagerTest extends TestLogger {

	@ClassRule
	public static final ZooKeeperResource ZOO_KEEPER_RESOURCE = new ZooKeeperResource();

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static final FiniteDuration TIMEOUT = FiniteDuration.apply(10L, TimeUnit.SECONDS);

	private static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() throws Exception {
		final Future<Terminated> terminationFuture = system.terminate();
		Await.ready(terminationFuture, TIMEOUT);
	}

	/**
	 * Tests that the {@link JobManager} releases all locked {@link JobGraph} if it loses
	 * leadership.
	 */
	@Test
	public void testJobGraphReleaseWhenLosingLeadership() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, ZOO_KEEPER_RESOURCE.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getAbsolutePath());

		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();

		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		final TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setJobMasterLeaderElectionService(HighAvailabilityServices.DEFAULT_JOB_ID, leaderElectionService);
		highAvailabilityServices.setSubmittedJobGraphStore(ZooKeeperUtils.createSubmittedJobGraphs(client, configuration));
		highAvailabilityServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		final CuratorFramework otherClient = ZooKeeperUtils.startCuratorFramework(configuration);
		final ZooKeeperSubmittedJobGraphStore otherSubmittedJobGraphStore = ZooKeeperUtils.createSubmittedJobGraphs(otherClient, configuration);
		otherSubmittedJobGraphStore.start(NoOpSubmittedJobGraphListener.INSTANCE);

		ActorRef jobManagerActorRef = null;
		try {
			jobManagerActorRef = JobManager.startJobManagerActors(
				configuration,
				system,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration)),
				Option.empty(),
				TestingJobManager.class,
				MemoryArchivist.class)._1();

			waitForActorToBeStarted(jobManagerActorRef, TIMEOUT);

			final ActorGateway jobManager = new AkkaActorGateway(jobManagerActorRef, HighAvailabilityServices.DEFAULT_LEADER_ID);

			leaderElectionService.isLeader(HighAvailabilityServices.DEFAULT_LEADER_ID).get();

			final JobGraph nonEmptyJobGraph = createNonEmptyJobGraph();

			final JobManagerMessages.SubmitJob submitJobMessage = new JobManagerMessages.SubmitJob(nonEmptyJobGraph, ListeningBehaviour.DETACHED);

			Await.result(jobManager.ask(submitJobMessage, TIMEOUT), TIMEOUT);

			Collection<JobID> jobIds = otherSubmittedJobGraphStore.getJobIds();

			final JobID jobId = nonEmptyJobGraph.getJobID();
			assertThat(jobIds, contains(jobId));

			// revoke the leadership
			leaderElectionService.notLeader();

			Await.result(jobManager.ask(TestingJobManagerMessages.getWaitForBackgroundTasksToFinish(), TIMEOUT), TIMEOUT);

			final SubmittedJobGraph recoveredJobGraph = akka.serialization.JavaSerializer.currentSystem().withValue(
				((ExtendedActorSystem) system),
				() -> otherSubmittedJobGraphStore.recoverJobGraph(jobId));

			assertThat(recoveredJobGraph, is(notNullValue()));

			otherSubmittedJobGraphStore.removeJobGraph(jobId);

			jobIds = otherSubmittedJobGraphStore.getJobIds();

			assertThat(jobIds, not(contains(jobId)));
		} finally {
			client.close();
			otherClient.close();

			if (jobManagerActorRef != null) {
				jobManagerActorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}

	private JobGraph createNonEmptyJobGraph() {
		final JobVertex noOpVertex = new JobVertex("NoOp vertex");
		noOpVertex.setInvokableClass(NoOpInvokable.class);
		final JobGraph jobGraph = new JobGraph(noOpVertex);
		jobGraph.setAllowQueuedScheduling(true);

		return jobGraph;
	}

	private void waitForActorToBeStarted(ActorRef jobManagerActorRef, FiniteDuration timeout) throws InterruptedException, java.util.concurrent.TimeoutException {
		Await.ready(Patterns.ask(jobManagerActorRef, new Identify(42), timeout.toMillis()), timeout);
	}

	enum NoOpSubmittedJobGraphListener implements SubmittedJobGraphStore.SubmittedJobGraphListener {
		INSTANCE;

		@Override
		public void onAddedJobGraph(JobID jobId) {
			// no op
		}

		@Override
		public void onRemovedJobGraph(JobID jobId) {
			// no op
		}
	}
}
