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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Implementation which archives an {@link AccessExecutionGraph} such that it stores the JSON
 * requests for all possible history server requests.
 */
class JsonResponseHistoryServerArchivist implements HistoryServerArchivist {

    private final JsonArchivist jsonArchivist;

    private final Path archivePath;

    private final Executor ioExecutor;

    JsonResponseHistoryServerArchivist(
            JsonArchivist jsonArchivist, Path archivePath, Executor ioExecutor) {
        this.jsonArchivist = Preconditions.checkNotNull(jsonArchivist);
        this.archivePath = Preconditions.checkNotNull(archivePath);
        this.ioExecutor = Preconditions.checkNotNull(ioExecutor);
    }

    @Override
    public CompletableFuture<Acknowledge> archiveExecutionGraph(
            AccessExecutionGraph executionGraph) {
        return CompletableFuture.runAsync(
                        ThrowingRunnable.unchecked(
                                () ->
                                        FsJobArchivist.archiveJob(
                                                archivePath,
                                                executionGraph.getJobID(),
                                                jsonArchivist.archiveJsonWithPath(executionGraph))),
                        ioExecutor)
                .thenApply(ignored -> Acknowledge.get());
    }
}
