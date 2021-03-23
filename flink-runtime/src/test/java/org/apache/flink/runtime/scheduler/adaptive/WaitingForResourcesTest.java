/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the WaitingForResources state. */
public class WaitingForResourcesTest extends TestLogger {
    private static final ResourceCounter RESOURCE_COUNTER =
            ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

    /** WaitingForResources is transitioning to Executing if there are enough resources. */
    @Test
    public void testTransitionToCreatingExecutionGraph() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> true);

            ctx.setExpectCreatingExecutionGraph();

            new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);
            // run delayed actions
            ctx.runScheduledTasks();
        }
    }

    @Test
    public void testNotEnoughResources() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);

            // we expect no state transition.
            wfr.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testNotifyNewResourcesAvailable() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false); // initially, not enough resources
            WaitingForResources wfr =
                    new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);
            ctx.setHasEnoughResources(() -> true); // make resources available
            ctx.setExpectCreatingExecutionGraph();
            wfr.notifyNewResourcesAvailable(); // .. and notify
        }
    }

    @Test
    public void testResourceTimeout() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);

            ctx.setExpectCreatingExecutionGraph();

            // immediately execute all scheduled runnables
            ctx.runScheduledTasks();
        }
    }

    @Test
    public void testTransitionToFinishedOnGlobalFailure() throws Exception {
        final String testExceptionString = "This is a test exception";
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);

            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
                        assertThat(archivedExecutionGraph.getFailureInfo(), notNullValue());
                        assertTrue(
                                archivedExecutionGraph
                                        .getFailureInfo()
                                        .getExceptionAsString()
                                        .contains(testExceptionString));
                    });

            wfr.handleGlobalFailure(new RuntimeException(testExceptionString));
        }
    }

    @Test
    public void testCancel() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.CANCELED));
                    }));
            wfr.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO);

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                        assertThat(archivedExecutionGraph.getFailureInfo(), notNullValue());
                    }));

            wfr.suspend(new RuntimeException("suspend"));
        }
    }

    private static class MockContext implements WaitingForResources.Context, AutoCloseable {

        private final StateValidator<Void> creatingExecutionGraphStateValidator =
                new StateValidator<>("executing");
        private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
                new StateValidator<>("finished");

        private Supplier<Boolean> hasEnoughResourcesSupplier = () -> false;
        private final List<ScheduledTask<Void>> scheduledTasks = new ArrayList<>();
        private boolean hasStateTransition = false;

        public void setHasEnoughResources(Supplier<Boolean> sup) {
            hasEnoughResourcesSupplier = sup;
        }

        void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            finishedStateValidator.expectInput(asserter);
        }

        void setExpectCreatingExecutionGraph() {
            creatingExecutionGraphStateValidator.expectInput(none -> {});
        }

        void runScheduledTasks() {
            for (ScheduledTask<Void> scheduledTask : scheduledTasks) {
                scheduledTask.execute();
            }
        }

        @Override
        public void close() throws Exception {
            creatingExecutionGraphStateValidator.close();
            finishedStateValidator.close();
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return new ArchivedExecutionGraphBuilder()
                    .setState(jobStatus)
                    .setFailureCause(cause == null ? null : new ErrorInfo(cause, 1337))
                    .build();
        }

        @Override
        public boolean hasEnoughResources(ResourceCounter desiredResources) {
            return hasEnoughResourcesSupplier.get();
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            final ScheduledTask<Void> scheduledTask =
                    new ScheduledTask<>(
                            () -> {
                                if (!hasStateTransition) {
                                    action.run();
                                }

                                return null;
                            },
                            delay.toMillis());

            scheduledTasks.add(scheduledTask);

            return scheduledTask;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedStateValidator.validateInput(archivedExecutionGraph);
            hasStateTransition = true;
        }

        @Override
        public void goToCreatingExecutionGraph() {
            creatingExecutionGraphStateValidator.validateInput(null);
            hasStateTransition = true;
        }

        public boolean hasStateTransition() {
            return hasStateTransition;
        }
    }

    static <T> Consumer<T> assertNonNull() {
        return (item) -> assertThat(item, notNullValue());
    }
}
