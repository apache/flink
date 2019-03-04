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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link JobManagerRunnerFactory} implementation for
 * testing purposes.
 */
class TestingJobManagerRunnerFactory implements JobManagerRunnerFactory {

	private final CompletableFuture<JobGraph> jobGraphFuture;
	private final CompletableFuture<ArchivedExecutionGraph> resultFuture;
	private final CompletableFuture<Void> terminationFuture;
	private final AtomicReference<Supplier<Exception>> failJobMasterCreationWith;

	TestingJobManagerRunnerFactory(
			CompletableFuture<JobGraph> jobGraphFuture,
			CompletableFuture<ArchivedExecutionGraph> resultFuture,
			CompletableFuture<Void> terminationFuture) {
		this(jobGraphFuture, resultFuture, terminationFuture, new AtomicReference<>());
	}

	TestingJobManagerRunnerFactory(
			CompletableFuture<JobGraph> jobGraphFuture,
			CompletableFuture<ArchivedExecutionGraph> resultFuture,
			CompletableFuture<Void> terminationFuture,
			AtomicReference<Supplier<Exception>> failJobMasterCreationWith) {
		this.jobGraphFuture = jobGraphFuture;
		this.resultFuture = resultFuture;
		this.terminationFuture = terminationFuture;
		this.failJobMasterCreationWith = failJobMasterCreationWith;
	}

	@Override
	public JobManagerRunner createJobManagerRunner(
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			JobManagerSharedServices jobManagerSharedServices,
			JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		final Supplier<Exception> exceptionSupplier = failJobMasterCreationWith.get();

		if (exceptionSupplier != null) {
			throw exceptionSupplier.get();
		} else {
			jobGraphFuture.complete(jobGraph);

			final JobManagerRunner mock = mock(JobManagerRunner.class);
			when(mock.getResultFuture()).thenReturn(resultFuture);
			when(mock.closeAsync()).thenReturn(terminationFuture);
			when(mock.getJobGraph()).thenReturn(jobGraph);

			return mock;
		}
	}
}
