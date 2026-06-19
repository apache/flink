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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * {@code TestingHistoryServerArchivist} implements {@link HistoryServerArchivist} to be used in
 * test contexts.
 */
public class TestingHistoryServerArchivist implements HistoryServerArchivist {

    private final BiFunction<ExecutionGraphInfo, ApplicationID, CompletableFuture<Acknowledge>>
            archiveExecutionGraphFunction;
    private final Function<ArchivedApplication, CompletableFuture<Acknowledge>>
            archiveApplicationFunction;

    public TestingHistoryServerArchivist(
            BiFunction<ExecutionGraphInfo, ApplicationID, CompletableFuture<Acknowledge>>
                    archiveExecutionGraphFunction,
            Function<ArchivedApplication, CompletableFuture<Acknowledge>>
                    archiveApplicationFunction) {
        this.archiveExecutionGraphFunction = archiveExecutionGraphFunction;
        this.archiveApplicationFunction = archiveApplicationFunction;
    }

    @Override
    public CompletableFuture<Acknowledge> archiveExecutionGraph(
            ExecutionGraphInfo executionGraphInfo, @Nullable ApplicationID applicationId) {
        return archiveExecutionGraphFunction.apply(executionGraphInfo, applicationId);
    }

    @Override
    public CompletableFuture<Acknowledge> archiveApplication(
            ArchivedApplication archivedApplication) {
        return archiveApplicationFunction.apply(archivedApplication);
    }

    public static TestingHistoryServerArchivist.Builder builder() {
        return new TestingHistoryServerArchivist.Builder();
    }

    public static class Builder {
        private BiFunction<ExecutionGraphInfo, ApplicationID, CompletableFuture<Acknowledge>>
                archiveExecutionGraphFunction =
                        (executionGraphInfo, applicationId) ->
                                CompletableFuture.completedFuture(Acknowledge.get());
        private Function<ArchivedApplication, CompletableFuture<Acknowledge>>
                archiveApplicationFunction =
                        ignored -> CompletableFuture.completedFuture(Acknowledge.get());

        public Builder setArchiveExecutionGraphFunction(
                BiFunction<ExecutionGraphInfo, ApplicationID, CompletableFuture<Acknowledge>>
                        function) {
            this.archiveExecutionGraphFunction = function;
            return this;
        }

        public Builder setArchiveApplicationFunction(
                Function<ArchivedApplication, CompletableFuture<Acknowledge>> function) {
            this.archiveApplicationFunction = function;
            return this;
        }

        public TestingHistoryServerArchivist build() {
            return new TestingHistoryServerArchivist(
                    archiveExecutionGraphFunction, archiveApplicationFunction);
        }
    }
}
