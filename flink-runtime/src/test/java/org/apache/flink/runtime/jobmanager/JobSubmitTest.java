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

import akka.actor.ActorSystem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.NetUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the JobManager handles Jobs correctly that fail in
 * the initialization during the submit phase.
 */
public class JobSubmitTest {

	private static final FiniteDuration timeout = new FiniteDuration(60000, TimeUnit.MILLISECONDS);

	private static ActorSystem jobManagerSystem;
	private static ActorGateway jmGateway;
	private static Configuration jmConfig;
	private static HighAvailabilityServices highAvailabilityServices;

	@BeforeClass
	public static void setupJobManager() {
		jmConfig = new Configuration();

		int port = NetUtils.getAvailablePort();

		jmConfig.setString(JobManagerOptions.ADDRESS, "localhost");
		jmConfig.setInteger(JobManagerOptions.PORT, port);

		scala.Option<Tuple2<String, Object>> listeningAddress = scala.Option.apply(new Tuple2<String, Object>("localhost", port));
		jobManagerSystem = AkkaUtils.createActorSystem(jmConfig, listeningAddress);

		highAvailabilityServices = new EmbeddedHaServices(TestingUtils.defaultExecutor());

		// only start JobManager (no ResourceManager)
		JobManager.startJobManagerActors(
			jmConfig,
			jobManagerSystem,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			highAvailabilityServices,
			NoOpMetricRegistry.INSTANCE,
			Option.empty(),
			JobManager.class,
			MemoryArchivist.class)._1();

		try {
			LeaderRetrievalService lrs = highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID);

			jmGateway = LeaderRetrievalUtils.retrieveLeaderGateway(
					lrs,
					jobManagerSystem,
					timeout
			);
		} catch (Exception e) {
			fail("Could not retrieve the JobManager gateway. " + e.getMessage());
		}
	}

	@AfterClass
	public static void teardownJobmanager() throws Exception {
		if (jobManagerSystem != null) {
			jobManagerSystem.shutdown();
		}

		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();
			highAvailabilityServices = null;
		}
	}

	@Test
	public void testFailureWhenJarBlobsMissing() {
		try {
			// create a simple job graph
			JobVertex jobVertex = new JobVertex("Test Vertex");
			jobVertex.setInvokableClass(NoOpInvokable.class);
			JobGraph jg = new JobGraph("test job", jobVertex);

			// add a reference to some non-existing BLOB to the job graph as a dependency
			jg.addUserJarBlobKey(new PermanentBlobKey());

			// submit the job
			Future<Object> submitFuture = jmGateway.ask(
					new JobManagerMessages.SubmitJob(
							jg,
							ListeningBehaviour.EXECUTION_RESULT),
					timeout);
			try {
				Await.result(submitFuture, timeout);
			}
			catch (JobExecutionException e) {
				// that is what we expect
				assertTrue(e.getCause() instanceof IOException);
			}
			catch (Exception e) {
				fail("Wrong exception type");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Verifies a correct error message when vertices with master initialization
	 * (input formats / output formats) fail.
	 */
	@Test
	public void testFailureWhenInitializeOnMasterFails() {
		try {
			// create a simple job graph

			JobVertex jobVertex = new JobVertex("Vertex that fails in initializeOnMaster") {

				private static final long serialVersionUID = -3540303593784587652L;

				@Override
				public void initializeOnMaster(ClassLoader loader) throws Exception {
					throw new RuntimeException("test exception");
				}
			};

			jobVertex.setInvokableClass(NoOpInvokable.class);
			JobGraph jg = new JobGraph("test job", jobVertex);

			// submit the job
			Future<Object> submitFuture = jmGateway.ask(
					new JobManagerMessages.SubmitJob(
							jg,
							ListeningBehaviour.EXECUTION_RESULT),
					timeout);
			try {
				Await.result(submitFuture, timeout);
			}
			catch (JobExecutionException e) {
				// that is what we expect
				// test that the exception nesting is not too deep
				assertTrue(e.getCause() instanceof RuntimeException);
			}
			catch (Exception e) {
				fail("Wrong exception type");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAnswerFailureWhenSavepointReadFails() throws Exception {
		// create a simple job graph
		JobGraph jg = createSimpleJobGraph();
		jg.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("pathThatReallyDoesNotExist..."));

		// submit the job
		Future<Object> submitFuture = jmGateway.ask(
			new JobManagerMessages.SubmitJob(jg, ListeningBehaviour.DETACHED), timeout);
		Object result = Await.result(submitFuture, timeout);
		assertEquals(JobManagerMessages.JobResultFailure.class, result.getClass());
	}

	private JobGraph createSimpleJobGraph() {
		JobVertex jobVertex = new JobVertex("Vertex");

		jobVertex.setInvokableClass(NoOpInvokable.class);
		List<JobVertexID> vertexIdList = Collections.singletonList(jobVertex.getID());

		JobGraph jg = new JobGraph("test job", jobVertex);
		jg.setSnapshotSettings(
			new JobCheckpointingSettings(
				vertexIdList,
				vertexIdList,
				vertexIdList,
				new CheckpointCoordinatorConfiguration(
					5000,
					5000,
					0L,
					10,
					CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					true),
				null));
		return jg;
	}
}
