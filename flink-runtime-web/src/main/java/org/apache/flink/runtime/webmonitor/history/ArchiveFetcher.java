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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@code ArchiveFetcher} class is an abstract class that defines the basic structure for
 * fetching job archives in the Flink History Server. Implementations of this class should provide
 * the logic for retrieving and processing job archives.
 *
 * <p>This class may be extended by specific implementations to support different storage backends
 * for job archives, such as local file systems or key-value stores.
 */
public abstract class ArchiveFetcher {

    /** Possible job archive operations in history-server. */
    public enum ArchiveEventType {
        /** Job archive was found in one refresh location and created in history server. */
        CREATED,
        /**
         * Job archive was deleted from one of refresh locations and deleted from history server.
         */
        DELETED
    }

    /** Representation of job archive event. */
    public static class ArchiveEvent {
        private final String jobID;
        private final ArchiveFetcher.ArchiveEventType operation;

        ArchiveEvent(String jobID, ArchiveFetcher.ArchiveEventType operation) {
            this.jobID = jobID;
            this.operation = operation;
        }

        public String getJobID() {
            return jobID;
        }

        public ArchiveFetcher.ArchiveEventType getType() {
            return operation;
        }
    }

    protected static final Logger LOG = LoggerFactory.getLogger(ArchiveFetcher.class);

    protected static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

    protected final List<HistoryServer.RefreshLocation> refreshDirs;
    protected final Consumer<ArchiveEvent> jobArchiveEventListener;
    protected final boolean processExpiredArchiveDeletion;
    protected final boolean processBeyondLimitArchiveDeletion;
    protected final int maxHistorySize;

    /** Cache of all available jobs identified by their id. */
    protected final Map<Path, Set<String>> cachedArchivesPerRefreshDirectory;

    protected ArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            Consumer<ArchiveEvent> jobArchiveEventListener,
            boolean cleanupExpiredArchives,
            int maxHistorySize) {
        this.refreshDirs = checkNotNull(refreshDirs);
        this.jobArchiveEventListener = jobArchiveEventListener;
        this.processExpiredArchiveDeletion = cleanupExpiredArchives;
        this.maxHistorySize = maxHistorySize;
        this.processBeyondLimitArchiveDeletion = this.maxHistorySize > 0;
        this.cachedArchivesPerRefreshDirectory = new HashMap<>();
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedArchivesPerRefreshDirectory.put(refreshDir.getPath(), new HashSet<>());
        }
    }

    abstract void fetchArchives();

    static FileStatus[] listArchives(FileSystem refreshFS, Path refreshDir) throws IOException {
        // contents of /:refreshDir
        FileStatus[] jobArchives = refreshFS.listStatus(refreshDir);
        if (jobArchives == null) {
            // the entire refreshDirectory was removed
            return new FileStatus[0];
        }

        Arrays.sort(
                jobArchives, Comparator.comparingLong(FileStatus::getModificationTime).reversed());

        return jobArchives;
    }

    static boolean isValidJobID(String jobId, Path refreshDir) {
        try {
            JobID.fromHexString(jobId);
            return true;
        } catch (IllegalArgumentException iae) {
            LOG.debug(
                    "Archive directory {} contained file with unexpected name {}. Ignoring file.",
                    refreshDir,
                    jobId,
                    iae);
            return false;
        }
    }

    abstract void processArchive(String jobID, Path jobArchivePath) throws IOException;

    List<ArchiveFetcher.ArchiveEvent> cleanupJobsBeyondSizeLimit(
            Map<Path, Set<Path>> jobArchivesToRemove) {
        Map<Path, Set<String>> allJobIdsToRemoveFromOverview = new HashMap<>();

        for (Map.Entry<Path, Set<Path>> pathSetEntry : jobArchivesToRemove.entrySet()) {
            HashSet<String> jobIdsToRemoveFromOverview = new HashSet<>();

            for (Path archive : pathSetEntry.getValue()) {
                jobIdsToRemoveFromOverview.add(archive.getName());
                try {
                    archive.getFileSystem().delete(archive, false);
                } catch (IOException ioe) {
                    LOG.warn("Could not delete old archive " + archive, ioe);
                }
            }
            allJobIdsToRemoveFromOverview.put(pathSetEntry.getKey(), jobIdsToRemoveFromOverview);
        }

        return cleanupExpiredJobs(allJobIdsToRemoveFromOverview);
    }

    List<ArchiveFetcher.ArchiveEvent> cleanupExpiredJobs(Map<Path, Set<String>> jobsToRemove) {

        List<ArchiveFetcher.ArchiveEvent> deleteLog = new ArrayList<>();
        LOG.info("Archive directories for jobs {} were deleted.", jobsToRemove);

        jobsToRemove.forEach(
                (refreshDir, archivesToRemove) -> {
                    cachedArchivesPerRefreshDirectory.get(refreshDir).removeAll(archivesToRemove);
                });
        jobsToRemove.values().stream()
                .flatMap(Set::stream)
                .forEach(
                        removedJobID -> {
                            deleteJobFiles(removedJobID);
                            deleteLog.add(
                                    new ArchiveFetcher.ArchiveEvent(
                                            removedJobID, ArchiveFetcher.ArchiveEventType.DELETED));
                        });

        return deleteLog;
    }

    abstract void deleteJobFiles(String jobID);

    static String convertLegacyJobOverview(String legacyOverview) throws IOException {
        JsonNode root = MAPPER.readTree(legacyOverview);
        JsonNode finishedJobs = root.get("finished");
        JsonNode job = finishedJobs.get(0);

        JobID jobId = JobID.fromHexString(job.get("jid").asText());
        String name = job.get("name").asText();
        JobStatus state = JobStatus.valueOf(job.get("state").asText());

        long startTime = job.get("start-time").asLong();
        long endTime = job.get("end-time").asLong();
        long duration = job.get("duration").asLong();
        long lastMod = job.get("last-modification").asLong();

        JsonNode tasks = job.get("tasks");
        int numTasks = tasks.get("total").asInt();
        JsonNode pendingNode = tasks.get("pending");
        // for flink version < 1.4 we have pending field,
        // when version >= 1.4 pending has been split into scheduled, deploying, and created.
        boolean versionLessThan14 = pendingNode != null;
        int created = 0;
        int scheduled;
        int deploying = 0;

        if (versionLessThan14) {
            // pending is a mix of CREATED/SCHEDULED/DEPLOYING
            // to maintain the correct number of task states we pick SCHEDULED
            scheduled = pendingNode.asInt();
        } else {
            created = tasks.get("created").asInt();
            scheduled = tasks.get("scheduled").asInt();
            deploying = tasks.get("deploying").asInt();
        }
        int running = tasks.get("running").asInt();
        int finished = tasks.get("finished").asInt();
        int canceling = tasks.get("canceling").asInt();
        int canceled = tasks.get("canceled").asInt();
        int failed = tasks.get("failed").asInt();

        int[] tasksPerState = new int[ExecutionState.values().length];
        tasksPerState[ExecutionState.CREATED.ordinal()] = created;
        tasksPerState[ExecutionState.SCHEDULED.ordinal()] = scheduled;
        tasksPerState[ExecutionState.DEPLOYING.ordinal()] = deploying;
        tasksPerState[ExecutionState.RUNNING.ordinal()] = running;
        tasksPerState[ExecutionState.FINISHED.ordinal()] = finished;
        tasksPerState[ExecutionState.CANCELING.ordinal()] = canceling;
        tasksPerState[ExecutionState.CANCELED.ordinal()] = canceled;
        tasksPerState[ExecutionState.FAILED.ordinal()] = failed;

        JobDetails jobDetails =
                new JobDetails(
                        jobId,
                        name,
                        startTime,
                        endTime,
                        duration,
                        state,
                        lastMod,
                        tasksPerState,
                        numTasks,
                        new HashMap<>());
        MultipleJobsDetails multipleJobsDetails =
                new MultipleJobsDetails(Collections.singleton(jobDetails));

        StringWriter sw = new StringWriter();
        MAPPER.writeValue(sw, multipleJobsDetails);
        return sw.toString();
    }
}
