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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

class MockStateWithExecutionGraphContext implements StateWithExecutionGraph.Context, AutoCloseable {

    private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
            new StateValidator<>("Finished");

    private final ManuallyTriggeredComponentMainThreadExecutor executor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    protected boolean hadStateTransition = false;

    public void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
        finishedStateValidator.expectInput(asserter);
    }

    @Override
    public void runIfState(State expectedState, Runnable action) {
        if (!hadStateTransition) {
            action.run();
        }
    }

    @Override
    public boolean isState(State expectedState) {
        throw new UnsupportedOperationException("Not covered by this test at the moment");
    }

    @Override
    public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
        finishedStateValidator.validateInput(archivedExecutionGraph);
        hadStateTransition = true;
    }

    @Override
    public ComponentMainThreadExecutor getMainThreadExecutor() {
        return executor;
    }

    @Override
    public void close() throws Exception {
        // trigger executor to make sure there are no outstanding state transitions
        executor.triggerAll();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
        finishedStateValidator.close();
    }

    protected final void assertNoStateTransition() {
        assertThat(hadStateTransition, is(false));
    }
}
