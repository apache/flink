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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Testing implementation of {@link JobManagerRunnerFactory} which returns a {@link TestingJobManagerRunner}.
 */
public class TestingJobManagerRunnerFactory implements JobManagerRunnerFactory {

	private final BlockingQueue<TestingJobManagerRunner> createdJobManagerRunner = new ArrayBlockingQueue<>(16);

	private int numBlockingJobManagerRunners;

	public TestingJobManagerRunnerFactory() {
		this(0);
	}

	public TestingJobManagerRunnerFactory(int numBlockingJobManagerRunners) {
		this.numBlockingJobManagerRunners = numBlockingJobManagerRunners;
	}

	@Override
	public TestingJobManagerRunner createJobManagerRunner(
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			JobManagerSharedServices jobManagerServices,
			JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		final TestingJobManagerRunner testingJobManagerRunner = createTestingJobManagerRunner(jobGraph);
		createdJobManagerRunner.offer(testingJobManagerRunner);

		return testingJobManagerRunner;
	}

	@Nonnull
	private TestingJobManagerRunner createTestingJobManagerRunner(JobGraph jobGraph) {
		final boolean blockingTermination;

		if (numBlockingJobManagerRunners > 0) {
			numBlockingJobManagerRunners--;
			blockingTermination = true;
		} else {
			blockingTermination = false;
		}

		return new TestingJobManagerRunner(jobGraph.getJobID(), blockingTermination);
	}

	public TestingJobManagerRunner takeCreatedJobManagerRunner() throws InterruptedException {
		return createdJobManagerRunner.take();
	}
}
