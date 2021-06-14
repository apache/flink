/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.logbundler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerActionQueryParameter;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerMessageParameters;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerStatus;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.rest.handler.resourcemanager.AbstractResourceManagerHandler.getResourceManagerGateway;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Handler which creates a downloadable archive containing all local (JobManager) and remote
 * (JobManager) log files.
 */
public class LogBundlerHandler
        extends AbstractHandler<RestfulGateway, EmptyRequestBody, LogBundlerMessageParameters> {

    // set high timeout for uploading files to the blob store
    private final Time rpcTimeout = Time.minutes(1);
    private final Object statusLock = new Object();
    private final ScheduledExecutorService executor;
    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    @Nullable private final File localLogDir;
    private final TransientBlobService transientBlobService;

    @Nullable private LogArchiver logArchiver;

    // null tmp dir indicates the bundler is disabled
    @Nullable private final String tmpDir;

    // messages for the REST API / frontend
    private volatile String message = "";

    // status of the bundler
    @GuardedBy("statusLock")
    private volatile Status status = Status.IDLE;

    /** States of the log bundler. */
    public enum Status {
        IDLE,
        PROCESSING,
        BUNDLE_READY,
        BUNDLE_FAILED
    }

    public LogBundlerHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            UntypedResponseMessageHeaders<EmptyRequestBody, LogBundlerMessageParameters>
                    messageHeaders,
            ScheduledExecutorService executor,
            Configuration clusterConfiguration,
            @Nullable File localLogDir,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            TransientBlobService transientBlobService) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.executor = executor;
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
        this.transientBlobService = transientBlobService;
        this.localLogDir = localLogDir;
        String[] tmpDirs = ConfigurationUtils.parseTempDirectories(clusterConfiguration);
        if (tmpDirs.length == 0) {
            this.message = "No tmp directory available for log bundler";
            log.error(message);
            this.tmpDir = null;
        } else {
            this.tmpDir = tmpDirs[0];
        }
    }

    @Override
    protected CompletableFuture<Void> respondToRequest(
            ChannelHandlerContext ctx,
            HttpRequest httpRequest,
            HandlerRequest<EmptyRequestBody, LogBundlerMessageParameters> handlerRequest,
            RestfulGateway gateway)
            throws RestHandlerException {
        List<String> queryParams =
                handlerRequest.getQueryParameter(LogBundlerActionQueryParameter.class);
        if (!queryParams.isEmpty()) {
            if (tmpDir == null) {
                throw new RestHandlerException(
                        this.message, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
            synchronized (statusLock) {
                final String action = queryParams.get(0);
                if ("download".equals(action)) {
                    if (status != Status.BUNDLE_READY) {
                        throw new RestHandlerException(
                                "There is no bundle ready to be downloaded",
                                HttpResponseStatus.BAD_REQUEST);
                    }
                    checkNotNull(
                            logArchiver,
                            "Assuming log archiver to be set in status " + Status.BUNDLE_READY);

                    try {
                        HandlerUtils.transferFile(ctx, logArchiver.getArchive(), httpRequest);
                        return CompletableFuture.completedFuture(null);
                    } catch (FlinkException e) {
                        log.warn("Error while transferring file", e);
                    }
                } else if ("trigger".equals(action)) {
                    if (status == Status.PROCESSING) {
                        throw new RestHandlerException(
                                "Unable to trigger log bundling while in status "
                                        + Status.PROCESSING,
                                HttpResponseStatus.BAD_REQUEST);
                    }
                    status = Status.PROCESSING;
                    executor.execute(this::collectAndCompressLogs);
                } else {
                    log.warn("Unknown action passed: '{}'", action);
                }
            }
        }

        // access status outside of lock to respond to status queries while processing.
        return HandlerUtils.sendResponse(
                ctx,
                httpRequest,
                new LogBundlerStatus(status, message),
                HttpResponseStatus.OK,
                responseHeaders);
    }

    private void collectAndCompressLogs() {
        synchronized (statusLock) {
            try {
                checkState(status == Status.PROCESSING, "Not in status PROCESSING");
                if (logArchiver != null) {
                    logArchiver.destroy();
                }
                final long creationTime = System.currentTimeMillis();
                File archive =
                        new File(tmpDir + File.separator + creationTime + "-flink-log-bundle.tgz");
                logArchiver = new LogArchiver(archive);
                archive.deleteOnExit();

                collectLocalLogs();
                collectLocalThreacollectLocalLogs() dDump();
                collectTaskManagerLogsAndThreadDumps();

                status = Status.BUNDLE_READY;
                message = "Created bundle at " + new Date(creationTime);
            } catch (Throwable throwable) {
                status = Status.BUNDLE_FAILED;
                message = "Error while creating bundle " + throwable.getMessage();
                log.warn("Error while creating bundle", throwable);
            } finally {
                try {
                    if (logArchiver != null) {
                        logArchiver.closeArchive();
                    }
                } catch (IOException e) {
                    log.warn("Error while closing log archive", e);
                }
            }
        }
    }

    private void collectLocalThreadDump() {
        Collection<ThreadInfo> threadDump = JvmUtils.createThreadDump();
        final Collection<ThreadDumpInfo.ThreadInfo> threadInfos =
                threadDump.stream()
                        .map(
                                threadInfo ->
                                        ThreadDumpInfo.ThreadInfo.create(
                                                threadInfo.getThreadName(), threadInfo.toString()))
                        .collect(Collectors.toList());
        addThreadDumps("jobmanager", threadInfos);
    }

    private void collectTaskManagerLogsAndThreadDumps()
            throws RestHandlerException, ExecutionException, InterruptedException {
        final ResourceManagerGateway resourceManagerGateway =
                getResourceManagerGateway(resourceManagerGatewayRetriever);
        Collection<TaskManagerInfo> taskManagers =
                resourceManagerGateway.requestTaskManagerInfo(rpcTimeout).get();
        // for this asynchronous collection of data, logArchiver.addArchiveEntry must be serialized
        Collection<CompletableFuture<Void>> collectionTaskFutures = new ArrayList<>();
        for (TaskManagerInfo taskManagerInfo : taskManagers) {
            CompletableFuture<Void> collectLogs =
                    requestLogs(resourceManagerGateway, taskManagerInfo)
                            .thenAccept(
                                    maybeLogs -> maybeLogs.ifPresent(this::addTaskManagerLogFile));
            collectionTaskFutures.add(collectLogs);

            CompletableFuture<Void> collectThreaddumps =
                    resourceManagerGateway
                            .requestThreadDump(taskManagerInfo.getResourceId(), rpcTimeout)
                            .thenAccept(
                                    threadDumpInfo ->
                                            addThreadDumps(
                                                    taskManagerInfo
                                                            .getResourceId()
                                                            .getResourceIdString(),
                                                    threadDumpInfo.getThreadInfos()));
            collectionTaskFutures.add(collectThreaddumps);
        }
        // wait for all operations to complete
        FutureUtils.combineAll(collectionTaskFutures).get();
    }

    private void addThreadDumps(
            String name, Collection<ThreadDumpInfo.ThreadInfo> threadDumpInfos) {
        checkNotNull(logArchiver, "Assuming log archiver to be set");
        StringBuffer sb = new StringBuffer();
        for (ThreadDumpInfo.ThreadInfo threadInfo : threadDumpInfos) {
            sb.append(threadInfo.toString());
        }

        byte[] entry = sb.toString().getBytes(StandardCharsets.UTF_8);
        try (InputStream input = new ByteArrayInputStream(entry)) {
            logArchiver.addArchiveEntry(
                    name + ".threaddump", input, entry.length, System.currentTimeMillis());
        } catch (IOException e) {
            log.warn("Error while collecting thread dumps for {}", name, e);
        }
    }

    private CompletableFuture<Optional<TaskManagerLogAndId>> requestLogs(
            ResourceManagerGateway resourceManagerGateway, TaskManagerInfo taskManager) {
        return resourceManagerGateway
                .requestTaskManagerFileUploadByType(
                        taskManager.getResourceId(), FileType.LOG, rpcTimeout)
                .thenApplyAsync(
                        tmLogBlobKey -> {
                            try {
                                return Optional.of(
                                        new TaskManagerLogAndId(
                                                taskManager.getResourceId(),
                                                transientBlobService.getFile(tmLogBlobKey)));
                            } catch (IOException e) {
                                log.warn("Error while retrieving log from TaskManager", e);
                                return Optional.empty();
                            }
                        },
                        executor);
    }

    private void addTaskManagerLogFile(TaskManagerLogAndId logFile) {
        checkNotNull(logArchiver, "Assuming log archiver to be set");
        try {
            logArchiver.addArchiveEntry(
                    "taskmanager-" + logFile.getTaskManagerId().toString() + ".log",
                    logFile.getLogFile());
        } catch (IOException e) {
            log.warn("Error while adding TaskManager log file to archive", e);
        }
    }

    private void collectLocalLogs() throws IOException {
        checkNotNull(logArchiver, "Assuming log archiver to be set");
        if (localLogDir == null) {
            return;
        }
        File[] localLogFiles = localLogDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (localLogFiles == null || localLogFiles.length == 0) {
            return;
        }
        for (File localLogFile : localLogFiles) {
            logArchiver.addArchiveEntry(localLogFile.getName(), localLogFile);
        }
    }

    private static class TaskManagerLogAndId {
        private final ResourceID taskManagerId;
        private final File logFile;

        public TaskManagerLogAndId(ResourceID taskManagerId, File logFile) {
            this.taskManagerId = taskManagerId;
            this.logFile = logFile;
        }

        public ResourceID getTaskManagerId() {
            return taskManagerId;
        }

        public File getLogFile() {
            return logFile;
        }
    }
}
