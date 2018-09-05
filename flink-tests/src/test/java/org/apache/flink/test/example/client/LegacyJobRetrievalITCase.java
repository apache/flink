/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobRetrievalException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import scala.collection.Seq;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests retrieval of a job from a running Flink cluster.
 */
public class LegacyJobRetrievalITCase extends TestLogger {

	private static final Semaphore lock = new Semaphore(1);

	private static FlinkMiniCluster cluster;

	@BeforeClass
	public static void before() {
		Configuration configuration = new Configuration();
		cluster = new TestingCluster(configuration, false);
		cluster.start();
	}

	@AfterClass
	public static void after() {
		cluster.stop();
		cluster = null;
	}

	@Test
	public void testJobRetrieval() throws Exception {
		final JobID jobID = new JobID();

		final JobVertex imalock = new JobVertex("imalock");
		imalock.setInvokableClass(SemaphoreInvokable.class);

		final JobGraph jobGraph = new JobGraph(jobID, "testjob", imalock);

		final ClusterClient<StandaloneClusterId> client = new StandaloneClusterClient(cluster.configuration(), cluster.highAvailabilityServices(), true);

		// acquire the lock to make sure that the job cannot complete until the job client
		// has been attached in resumingThread
		lock.acquire();
		client.runDetached(jobGraph, LegacyJobRetrievalITCase.class.getClassLoader());
		final AtomicReference<Throwable> error = new AtomicReference<>();

		final Thread resumingThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					assertNotNull(client.retrieveJob(jobID));
				} catch (Throwable e) {
					error.set(e);
				}
			}
		}, "Flink-Job-Retriever");

		final Seq<ActorSystem> actorSystemSeq = cluster.jobManagerActorSystems().get();
		final ActorSystem actorSystem = actorSystemSeq.last();
		JavaTestKit testkit = new JavaTestKit(actorSystem);

		final ActorRef jm = cluster.getJobManagersAsJava().get(0);
		// wait until client connects
		jm.tell(TestingJobManagerMessages.getNotifyWhenClientConnects(), testkit.getRef());
		// confirm registration
		testkit.expectMsgEquals(true);

		// kick off resuming
		resumingThread.start();

		// wait for client to connect
		testkit.expectMsgAllOf(
			TestingJobManagerMessages.getClientConnected(),
			TestingJobManagerMessages.getClassLoadingPropsDelivered());

		// client has connected, we can release the lock
		lock.release();

		resumingThread.join();

		Throwable exception = error.get();
		if (exception != null) {
			throw new AssertionError(exception);
		}
	}

	@Test
	public void testNonExistingJobRetrieval() throws Exception {
		final JobID jobID = new JobID();
		ClusterClient<StandaloneClusterId> client = new StandaloneClusterClient(cluster.configuration());

		try {
			client.retrieveJob(jobID);
			fail();
		} catch (JobRetrievalException ignored) {
			// this is what we want
		}
	}

	/**
	 * Invokable that waits on {@link #lock} to be released and finishes afterwards.
	 *
	 * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
	 */
	public static class SemaphoreInvokable extends AbstractInvokable {

		public SemaphoreInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			lock.acquire();
			lock.release();
		}
	}

}
