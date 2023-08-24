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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.function.Consumer;

/** Mock the {@link StateWithoutExecutionGraph.Context}. */
class MockStateWithoutExecutionGraphContext
        implements StateWithoutExecutionGraph.Context, AfterEachCallback {

    private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
            new StateValidator<>("Finished");

    private boolean hasStateTransition = false;

    public void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
        finishedStateValidator.expectInput(asserter);
    }

    @Override
    public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
        finishedStateValidator.validateInput(archivedExecutionGraph);
        registerStateTransition();
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
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        finishedStateValidator.close();
    }

    public boolean hasStateTransition() {
        return hasStateTransition;
    }

    public void registerStateTransition() {
        hasStateTransition = true;
    }
}
