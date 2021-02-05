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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

class MockStateWithExecutionGraphContext implements StateWithExecutionGraph.Context, AutoCloseable {

    private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
            new StateValidator<>("finished");

    Function<State, Boolean> expectedStateChecker =
            (ign) -> {
                throw new UnsupportedOperationException("Remember to set me");
            };

    private final ManuallyTriggeredComponentMainThreadExecutor executor =
            new ManuallyTriggeredComponentMainThreadExecutor();

    public void setExpectedStateChecker(Function<State, Boolean> function) {
        this.expectedStateChecker = function;
    }

    public void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
        finishedStateValidator.expectInput(asserter);
    }

    @Override
    public void runIfState(State expectedState, Runnable action) {
        if (expectedStateChecker.apply(expectedState)) {
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
}
