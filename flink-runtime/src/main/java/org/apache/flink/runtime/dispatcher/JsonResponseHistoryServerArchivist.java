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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.history.ArchivePathUtils;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.history.ApplicationJsonArchivist;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation which archives an {@link AccessExecutionGraph} such that it stores the JSON
 * requests for all possible history server requests.
 */
class JsonResponseHistoryServerArchivist implements HistoryServerArchivist {

    private final Configuration configuration;

    private final JsonArchivist jsonArchivist;

    private final ApplicationJsonArchivist applicationJsonArchivist;

    private final Executor ioExecutor;

    JsonResponseHistoryServerArchivist(
            Configuration configuration,
            JsonArchivist jsonArchivist,
            ApplicationJsonArchivist applicationJsonArchivist,
            Executor ioExecutor) {
        this.configuration = checkNotNull(configuration);
        this.jsonArchivist = checkNotNull(jsonArchivist);
        this.applicationJsonArchivist = checkNotNull(applicationJsonArchivist);
        this.ioExecutor = checkNotNull(ioExecutor);
    }

    @Override
    public CompletableFuture<Acknowledge> archiveExecutionGraph(
            ExecutionGraphInfo executionGraphInfo, @Nullable ApplicationID applicationId) {
        Path jobArchivePath =
                ArchivePathUtils.getJobArchivePath(
                        configuration, executionGraphInfo.getJobId(), applicationId);

        return CompletableFuture.runAsync(
                        ThrowingRunnable.unchecked(
                                () ->
                                        FsJsonArchivist.writeArchivedJsons(
                                                jobArchivePath,
                                                jsonArchivist.archiveJsonWithPath(
                                                        executionGraphInfo))),
                        ioExecutor)
                .thenApply(ignored -> Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> archiveApplication(
            ArchivedApplication archivedApplication) {
        Path applicationArchivePath =
                ArchivePathUtils.getApplicationArchivePath(
                        configuration, archivedApplication.getApplicationId());

        return CompletableFuture.runAsync(
                        ThrowingRunnable.unchecked(
                                () ->
                                        FsJsonArchivist.writeArchivedJsons(
                                                applicationArchivePath,
                                                applicationJsonArchivist.archiveApplicationWithPath(
                                                        archivedApplication))),
                        ioExecutor)
                .thenApply(ignored -> Acknowledge.get());
    }
}
