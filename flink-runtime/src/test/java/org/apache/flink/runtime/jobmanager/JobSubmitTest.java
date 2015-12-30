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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.NetUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the JobManager handles Jobs correctly that fail in
 * the initialization during the submit phase.
 */
public class JobSubmitTest {

	private static final FiniteDuration timeout = new FiniteDuration(5000, TimeUnit.MILLISECONDS);

	private static ActorSystem jobManagerSystem;
	private static ActorGateway jmGateway;

	@BeforeClass
	public static void setupJobManager() {
		Configuration config = new Configuration();

		int port = NetUtils.getAvailablePort();

		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port);

		scala.Option<Tuple2<String, Object>> listeningAddress = scala.Option.apply(new Tuple2<String, Object>("localhost", port));
		jobManagerSystem = AkkaUtils.createActorSystem(config, listeningAddress);
		ActorRef jobManagerActorRef = JobManager.startJobManagerActors(
				config,
				jobManagerSystem,
				JobManager.class,
				MemoryArchivist.class)._1();

		try {
			LeaderRetrievalService lrs = LeaderRetrievalUtils.createLeaderRetrievalService(config);

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
	public static void teardownJobmanager() {
		if (jobManagerSystem != null) {
			jobManagerSystem.shutdown();
		}
	}

	@Test
	public void testFailureWhenJarBlobsMissing() {
		try {
			// create a simple job graph
			JobVertex jobVertex = new JobVertex("Test Vertex");
			jobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
			JobGraph jg = new JobGraph("test job", jobVertex);

			// request the blob port from the job manager
			Future<Object> future = jmGateway.ask(JobManagerMessages.getRequestBlobManagerPort(), timeout);
			int blobPort = (Integer) Await.result(future, timeout);

			// upload two dummy bytes and add their keys to the job graph as dependencies
			BlobKey key1, key2;
			BlobClient bc = new BlobClient(new InetSocketAddress("localhost", blobPort));
			try {
				key1 = bc.put(new byte[10]);
				key2 = bc.put(new byte[10]);

				// delete one of the blobs to make sure that the startup failed
				bc.delete(key2);
			}
			finally {
				bc.close();
			}

			jg.addBlob(key1);
			jg.addBlob(key2);

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

				@Override
				public void initializeOnMaster(ClassLoader loader) throws Exception {
					throw new RuntimeException("test exception");
				}
			};

			jobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
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
}
