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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class JobManagerMetricTest {
	/**
	 * Tests that metrics registered on the JobManager are actually accessible.
	 *
	 * @throws Exception
	 */
	@Test
	public void testJobManagerMetricAccess() throws Exception {
		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		Configuration flinkConfiguration = new Configuration();

		flinkConfiguration.setString(ConfigConstants.METRICS_REPORTER_CLASS, "org.apache.flink.metrics.reporter.JMXReporter");
		flinkConfiguration.setString(ConfigConstants.METRICS_SCOPE_NAMING_JM_JOB, "jobmanager.<job_name>");
		flinkConfiguration.setString(ConfigConstants.METRICS_REPORTER_ARGUMENTS, "--port 9060-9075");

		TestingCluster flink = new TestingCluster(flinkConfiguration);

		try {
			flink.start();

			JobVertex sourceJobVertex = new JobVertex("Source");
			sourceJobVertex.setInvokableClass(BlockingInvokable.class);

			JobGraph jobGraph = new JobGraph("TestingJob", sourceJobVertex);
			jobGraph.setSnapshotSettings(new JobSnapshottingSettings(
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				500, 500, 50, 5));

			flink.waitForActorsToBeAlive();

			flink.submitJobDetached(jobGraph);

			Future<Object> jobRunning = flink.getLeaderGateway(deadline.timeLeft())
				.ask(new TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobGraph.getJobID()), deadline.timeLeft());
			Await.ready(jobRunning, deadline.timeLeft());

			MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName1 = new ObjectName("org.apache.flink.metrics:key0=jobmanager,key1=TestingJob,name=lastCheckpointSize");
			assertEquals(-1L, mBeanServer.getAttribute(objectName1, "Value"));

			Future<Object> jobFinished = flink.getLeaderGateway(deadline.timeLeft())
				.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID()), deadline.timeLeft());

			BlockingInvokable.unblock();

			// wait til the job has finished
			Await.ready(jobFinished, deadline.timeLeft());
		} finally {
			flink.stop();
		}
	}

	public static class BlockingInvokable extends AbstractInvokable {
		private static boolean blocking = true;
		private static final Object lock = new Object();

		@Override
		public void invoke() throws Exception {
			while (blocking) {
				synchronized (lock) {
					lock.wait();
				}
			}
		}

		public static void unblock() {
			blocking = false;

			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}
}
