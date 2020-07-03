/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DefaultExecutionDeploymentReconciler}.
 */
public class DefaultExecutionDeploymentReconcilerTest extends TestLogger {

	@Test
	public void testMatchingDeployments() throws Exception {
		runTest((reconciler, missingFuture, unknownFuture) -> {
			ResourceID resourceId = ResourceID.generate();
			ExecutionAttemptID attemptId = new ExecutionAttemptID();

			reconciler.reconcileExecutionDeployments(
				resourceId,
				new ExecutionDeploymentReport(Collections.singleton(attemptId)),
				Collections.singleton(attemptId));

			assertFalse(missingFuture.isDone());
			assertFalse(unknownFuture.isDone());
		});
	}

	@Test
	public void testMissingDeployments() throws Exception {
		runTest((reconciler, missingFuture, unknownFuture) -> {
			ResourceID resourceId = ResourceID.generate();
			ExecutionAttemptID attemptId = new ExecutionAttemptID();

			reconciler.reconcileExecutionDeployments(
				resourceId,
				new ExecutionDeploymentReport(Collections.emptySet()),
				Collections.singleton(attemptId));

			assertFalse(unknownFuture.isDone());
			assertThat(missingFuture.get(), is(Tuple2.of(attemptId, resourceId)));
		});
	}

	@Test
	public void testUnknownDeployments() throws Exception {
		runTest((reconciler, missingFuture, unknownFuture) -> {
			ResourceID resourceId = ResourceID.generate();
			ExecutionAttemptID attemptId = new ExecutionAttemptID();

			reconciler.reconcileExecutionDeployments(
				resourceId,
				new ExecutionDeploymentReport(Collections.singleton(attemptId)),
				Collections.emptySet());

			assertFalse(missingFuture.isDone());
			assertThat(unknownFuture.get(), is(Tuple2.of(attemptId, resourceId)));
		});
	}

	@Test
	public void testMissingAndUnknownDeployments() throws Exception {
		runTest((reconciler, missingFuture, unknownFuture) -> {
			ResourceID resourceId = ResourceID.generate();
			ExecutionAttemptID unknownId = new ExecutionAttemptID();
			ExecutionAttemptID missingId = new ExecutionAttemptID();
			ExecutionAttemptID matchingId = new ExecutionAttemptID();

			reconciler.reconcileExecutionDeployments(
				resourceId,
				new ExecutionDeploymentReport(new HashSet<>(Arrays.asList(unknownId, matchingId))),
				new HashSet<>(Arrays.asList(missingId, matchingId)));

			assertThat(missingFuture.get(), is(missingId));
			assertThat(unknownFuture.get(), is(Tuple2.of(unknownId, resourceId)));
		});
	}

	private static void runTest(TestRun test) throws Exception {
		CompletableFuture<Tuple2<ExecutionAttemptID, ResourceID>> missingFuture = new CompletableFuture<>();
		CompletableFuture<Tuple2<ExecutionAttemptID, ResourceID>> unknownFuture = new CompletableFuture<>();

		ExecutionDeploymentReconciliationHandler handler = new ExecutionDeploymentReconciliationHandler() {
			@Override
			public void onMissingDeploymentOf(ExecutionAttemptID executionAttemptId, ResourceID hostingTaskExecutor) {
				missingFuture.complete(Tuple2.of(executionAttemptId, hostingTaskExecutor));
			}

			@Override
			public void onUnknownDeploymentOf(ExecutionAttemptID executionAttemptId, ResourceID hostingTaskExecutor) {
				unknownFuture.complete(Tuple2.of(executionAttemptId, hostingTaskExecutor));
			}
		};

		DefaultExecutionDeploymentReconciler reconciler = new DefaultExecutionDeploymentReconciler(handler);

		test.run(reconciler, missingFuture, unknownFuture);
	}

	@FunctionalInterface
	private interface TestRun {
		void run(ExecutionDeploymentReconciler reconciler, CompletableFuture<Tuple2<ExecutionAttemptID, ResourceID>> missingFuture, CompletableFuture<Tuple2<ExecutionAttemptID, ResourceID>> unknownFuture) throws Exception;
	}

}
