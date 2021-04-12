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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Testing implementation of {@link TaskManagerActions}. */
public class TestingTaskManagerActions implements TaskManagerActions {

    private final BiConsumer<String, Throwable> notifyFatalErrorConsumer;

    private final BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer;

    private final Consumer<TaskExecutionState> updateTaskExecutionStateConsumer;

    private TestingTaskManagerActions(
            BiConsumer<String, Throwable> notifyFatalErrorConsumer,
            BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer,
            Consumer<TaskExecutionState> updateTaskExecutionStateConsumer) {
        this.notifyFatalErrorConsumer = notifyFatalErrorConsumer;
        this.failTaskConsumer = failTaskConsumer;
        this.updateTaskExecutionStateConsumer = updateTaskExecutionStateConsumer;
    }

    @Override
    public void notifyFatalError(String message, Throwable cause) {
        notifyFatalErrorConsumer.accept(message, cause);
    }

    @Override
    public void failTask(ExecutionAttemptID executionAttemptID, Throwable cause) {
        failTaskConsumer.accept(executionAttemptID, cause);
    }

    @Override
    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        updateTaskExecutionStateConsumer.accept(taskExecutionState);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private BiConsumer<String, Throwable> notifyFatalErrorConsumer = (ignoredA, ignoredB) -> {};
        private BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer =
                (ignoredA, ignoredB) -> {};
        private Consumer<TaskExecutionState> updateTaskExecutionStateConsumer = ignored -> {};

        private Builder() {}

        public Builder setNotifyFatalErrorConsumer(
                BiConsumer<String, Throwable> notifyFatalErrorConsumer) {
            this.notifyFatalErrorConsumer = notifyFatalErrorConsumer;
            return this;
        }

        public Builder setFailTaskConsumer(
                BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer) {
            this.failTaskConsumer = failTaskConsumer;
            return this;
        }

        public Builder setUpdateTaskExecutionStateConsumer(
                Consumer<TaskExecutionState> updateTaskExecutionStateConsumer) {
            this.updateTaskExecutionStateConsumer = updateTaskExecutionStateConsumer;
            return this;
        }

        public TestingTaskManagerActions build() {
            return new TestingTaskManagerActions(
                    notifyFatalErrorConsumer, failTaskConsumer, updateTaskExecutionStateConsumer);
        }
    }
}
