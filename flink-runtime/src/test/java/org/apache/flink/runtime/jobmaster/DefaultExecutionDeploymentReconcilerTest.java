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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.clusterframework.types.ResourceID.generate;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;

/** Tests for {@link DefaultExecutionDeploymentReconciler}. */
public class DefaultExecutionDeploymentReconcilerTest extends TestLogger {

    @Test
    public void testMatchingDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID attemptId = new ExecutionAttemptID();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(Collections.singleton(attemptId)),
                Collections.singletonMap(attemptId, ExecutionDeploymentState.DEPLOYED));

        assertThat(handler.getMissingExecutions(), empty());
        assertThat(handler.getUnknownExecutions(), empty());
    }

    @Test
    public void testMissingDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID attemptId = new ExecutionAttemptID();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(Collections.emptySet()),
                Collections.singletonMap(attemptId, ExecutionDeploymentState.DEPLOYED));

        assertThat(handler.getUnknownExecutions(), empty());
        assertThat(handler.getMissingExecutions(), hasItem(attemptId));
    }

    @Test
    public void testUnknownDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID attemptId = new ExecutionAttemptID();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(Collections.singleton(attemptId)),
                Collections.emptyMap());

        assertThat(handler.getMissingExecutions(), empty());
        assertThat(handler.getUnknownExecutions(), hasItem(attemptId));
    }

    @Test
    public void testMissingAndUnknownDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID unknownId = new ExecutionAttemptID();
        ExecutionAttemptID missingId = new ExecutionAttemptID();
        ExecutionAttemptID matchingId = new ExecutionAttemptID();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(new HashSet<>(Arrays.asList(unknownId, matchingId))),
                Stream.of(missingId, matchingId)
                        .collect(Collectors.toMap(x -> x, x -> ExecutionDeploymentState.DEPLOYED)));

        assertThat(handler.getMissingExecutions(), hasItem(missingId));
        assertThat(handler.getUnknownExecutions(), hasItem(unknownId));
    }

    @Test
    public void testPendingDeployments() {
        TestingExecutionDeploymentReconciliationHandler handler =
                new TestingExecutionDeploymentReconciliationHandler();

        DefaultExecutionDeploymentReconciler reconciler =
                new DefaultExecutionDeploymentReconciler(handler);

        ResourceID resourceId = generate();
        ExecutionAttemptID matchingId = new ExecutionAttemptID();
        ExecutionAttemptID unknownId = new ExecutionAttemptID();
        ExecutionAttemptID missingId = new ExecutionAttemptID();

        reconciler.reconcileExecutionDeployments(
                resourceId,
                new ExecutionDeploymentReport(new HashSet<>(Arrays.asList(matchingId, unknownId))),
                Stream.of(matchingId, missingId)
                        .collect(Collectors.toMap(x -> x, x -> ExecutionDeploymentState.PENDING)));

        assertThat(handler.getMissingExecutions(), empty());
        assertThat(handler.getUnknownExecutions(), hasItem(unknownId));
    }

    private static class TestingExecutionDeploymentReconciliationHandler
            implements ExecutionDeploymentReconciliationHandler {
        private final Collection<ExecutionAttemptID> missingExecutions = new ArrayList<>();
        private final Collection<ExecutionAttemptID> unknownExecutions = new ArrayList<>();

        @Override
        public void onMissingDeploymentsOf(
                Collection<ExecutionAttemptID> executionAttemptIds,
                ResourceID hostingTaskExecutor) {
            missingExecutions.addAll(executionAttemptIds);
        }

        @Override
        public void onUnknownDeploymentsOf(
                Collection<ExecutionAttemptID> executionAttemptIds,
                ResourceID hostingTaskExecutor) {
            unknownExecutions.addAll(executionAttemptIds);
        }

        public Collection<ExecutionAttemptID> getMissingExecutions() {
            return missingExecutions;
        }

        public Collection<ExecutionAttemptID> getUnknownExecutions() {
            return unknownExecutions;
        }
    }
}
