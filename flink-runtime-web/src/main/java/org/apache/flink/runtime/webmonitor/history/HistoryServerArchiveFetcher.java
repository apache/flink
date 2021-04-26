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
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.runtime.util.Runnables;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is used by the {@link HistoryServer} to fetch the job archives that are located at
 * {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are polled in regular
 * intervals, defined by {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}.
 *
 * <p>The archives are downloaded and expanded into a file structure analog to the REST API.
 */
class HistoryServerArchiveFetcher {

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
        private final ArchiveEventType operation;

        ArchiveEvent(String jobID, ArchiveEventType operation) {
            this.jobID = jobID;
            this.operation = operation;
        }

        public String getJobID() {
            return jobID;
        }

        public ArchiveEventType getType() {
            return operation;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(HistoryServerArchiveFetcher.class);

    private static final JsonFactory jacksonFactory = new JsonFactory();
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("Flink-HistoryServer-ArchiveFetcher"));
    private final JobArchiveFetcherTask fetcherTask;
    private final long refreshIntervalMillis;

    HistoryServerArchiveFetcher(
            long refreshIntervalMillis,
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ArchiveEvent> jobArchiveEventListener,
            boolean cleanupExpiredArchives,
            int maxHistorySize)
            throws IOException {
        this.refreshIntervalMillis = refreshIntervalMillis;
        this.fetcherTask =
                new JobArchiveFetcherTask(
                        refreshDirs,
                        webDir,
                        jobArchiveEventListener,
                        cleanupExpiredArchives,
                        maxHistorySize);
        if (LOG.isInfoEnabled()) {
            for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
                LOG.info("Monitoring directory {} for archived jobs.", refreshDir.getPath());
            }
        }
    }

    void start() {
        final Runnable guardedTask =
                Runnables.withUncaughtExceptionHandler(
                        fetcherTask, FatalExitExceptionHandler.INSTANCE);
        executor.scheduleWithFixedDelay(
                guardedTask, 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    void stop() {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            executor.shutdownNow();
        }
    }

    /**
     * {@link TimerTask} that polls the directories configured as {@link
     * HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS} for new job archives. Removes existing
     * archives from these directories and the cache if configured by {@link
     * HistoryServerOptions#HISTORY_SERVER_CLEANUP_EXPIRED_JOBS} or {@link
     * HistoryServerOptions#HISTORY_SERVER_RETAINED_JOBS}
     */
    static class JobArchiveFetcherTask extends TimerTask {

        private final List<HistoryServer.RefreshLocation> refreshDirs;
        private final Consumer<ArchiveEvent> jobArchiveEventListener;
        private final boolean processExpiredArchiveDeletion;
        private final boolean processBeyondLimitArchiveDeletion;
        private final int maxHistorySize;

        /** Cache of all available jobs identified by their id. */
        private final Set<String> cachedArchives;

        private final File webDir;
        private final File webJobDir;
        private final File webOverviewDir;

        private static final String JSON_FILE_ENDING = ".json";

        JobArchiveFetcherTask(
                List<HistoryServer.RefreshLocation> refreshDirs,
                File webDir,
                Consumer<ArchiveEvent> jobArchiveEventListener,
                boolean processExpiredArchiveDeletion,
                int maxHistorySize)
                throws IOException {
            this.refreshDirs = checkNotNull(refreshDirs);
            this.jobArchiveEventListener = jobArchiveEventListener;
            this.processExpiredArchiveDeletion = processExpiredArchiveDeletion;
            this.maxHistorySize = maxHistorySize;
            this.processBeyondLimitArchiveDeletion = this.maxHistorySize > 0;
            this.cachedArchives = new HashSet<>();
            this.webDir = checkNotNull(webDir);
            this.webJobDir = new File(webDir, "jobs");
            Files.createDirectories(webJobDir.toPath());
            this.webOverviewDir = new File(webDir, "overviews");
            Files.createDirectories(webOverviewDir.toPath());
            updateJobOverview(webOverviewDir, webDir);
        }

        @Override
        public void run() {
            try {
                LOG.debug("Starting archive fetching.");
                List<ArchiveEvent> events = new ArrayList<>();
                Set<String> jobsToRemove = new HashSet<>(cachedArchives);
                Set<Path> archivesBeyondSizeLimit = new HashSet<>();
                for (HistoryServer.RefreshLocation refreshLocation : refreshDirs) {
                    Path refreshDir = refreshLocation.getPath();
                    FileSystem refreshFS = refreshLocation.getFs();
                    LOG.debug("Checking archive directory {}.", refreshDir);

                    // contents of /:refreshDir
                    FileStatus[] jobArchives;
                    try {
                        jobArchives = refreshFS.listStatus(refreshDir);
                    } catch (IOException e) {
                        LOG.error(
                                "Failed to access job archive location for path {}.",
                                refreshDir,
                                e);
                        continue;
                    }
                    if (jobArchives == null) {
                        continue;
                    }

                    Arrays.sort(
                            jobArchives,
                            Comparator.comparingLong(FileStatus::getModificationTime).reversed());

                    int historySize = 0;
                    for (FileStatus jobArchive : jobArchives) {
                        Path jobArchivePath = jobArchive.getPath();
                        String jobID = jobArchivePath.getName();
                        try {
                            JobID.fromHexString(jobID);
                        } catch (IllegalArgumentException iae) {
                            LOG.debug(
                                    "Archive directory {} contained file with unexpected name {}. Ignoring file.",
                                    refreshDir,
                                    jobID,
                                    iae);
                            continue;
                        }

                        jobsToRemove.remove(jobID);

                        historySize++;
                        if (historySize > maxHistorySize && processBeyondLimitArchiveDeletion) {
                            archivesBeyondSizeLimit.add(jobArchivePath);
                            continue;
                        }

                        if (cachedArchives.contains(jobID)) {
                            LOG.trace(
                                    "Ignoring archive {} because it was already fetched.",
                                    jobArchivePath);
                        } else {
                            LOG.info("Processing archive {}.", jobArchivePath);
                            try {
                                for (ArchivedJson archive :
                                        FsJobArchivist.getArchivedJsons(jobArchive.getPath())) {
                                    String path = archive.getPath();
                                    String json = archive.getJson();

                                    File target;
                                    if (path.equals(JobsOverviewHeaders.URL)) {
                                        target = new File(webOverviewDir, jobID + JSON_FILE_ENDING);
                                    } else if (path.equals("/joboverview")) { // legacy path
                                        LOG.debug("Migrating legacy archive {}", jobArchivePath);
                                        json = convertLegacyJobOverview(json);
                                        target = new File(webOverviewDir, jobID + JSON_FILE_ENDING);
                                    } else {
                                        // this implicitly writes into webJobDir
                                        target = new File(webDir, path + JSON_FILE_ENDING);
                                    }

                                    java.nio.file.Path parent = target.getParentFile().toPath();

                                    try {
                                        Files.createDirectories(parent);
                                    } catch (FileAlreadyExistsException ignored) {
                                        // there may be left-over directories from the previous
                                        // attempt
                                    }

                                    java.nio.file.Path targetPath = target.toPath();

                                    // We overwrite existing files since this may be another attempt
                                    // at fetching this archive.
                                    // Existing files may be incomplete/corrupt.
                                    Files.deleteIfExists(targetPath);

                                    Files.createFile(target.toPath());
                                    try (FileWriter fw = new FileWriter(target)) {
                                        fw.write(json);
                                        fw.flush();
                                    }
                                }
                                events.add(new ArchiveEvent(jobID, ArchiveEventType.CREATED));
                                cachedArchives.add(jobID);
                                LOG.info("Processing archive {} finished.", jobArchivePath);
                            } catch (IOException e) {
                                LOG.error(
                                        "Failure while fetching/processing job archive for job {}.",
                                        jobID,
                                        e);
                                deleteJobFiles(jobID);
                            }
                        }
                    }
                }

                if (!jobsToRemove.isEmpty() && processExpiredArchiveDeletion) {
                    events.addAll(cleanupExpiredJobs(jobsToRemove));
                }
                if (!archivesBeyondSizeLimit.isEmpty() && processBeyondLimitArchiveDeletion) {
                    events.addAll(cleanupJobsBeyondSizeLimit(archivesBeyondSizeLimit));
                }
                if (!events.isEmpty()) {
                    updateJobOverview(webOverviewDir, webDir);
                }
                events.forEach(jobArchiveEventListener::accept);
                LOG.debug("Finished archive fetching.");
            } catch (Exception e) {
                LOG.error("Critical failure while fetching/processing job archives.", e);
            }
        }

        private List<ArchiveEvent> cleanupJobsBeyondSizeLimit(Set<Path> jobArchivesToRemove) {
            Set<String> jobIdsToRemoveFromOverview = new HashSet<>();
            for (Path archive : jobArchivesToRemove) {
                jobIdsToRemoveFromOverview.add(archive.getName());
                try {
                    archive.getFileSystem().delete(archive, false);
                } catch (IOException ioe) {
                    LOG.warn("Could not delete old archive " + archive, ioe);
                }
            }
            return cleanupExpiredJobs(jobIdsToRemoveFromOverview);
        }

        private List<ArchiveEvent> cleanupExpiredJobs(Set<String> jobsToRemove) {

            List<ArchiveEvent> deleteLog = new ArrayList<>();
            LOG.info("Archive directories for jobs {} were deleted.", jobsToRemove);

            cachedArchives.removeAll(jobsToRemove);
            jobsToRemove.forEach(
                    removedJobID -> {
                        deleteJobFiles(removedJobID);
                        deleteLog.add(new ArchiveEvent(removedJobID, ArchiveEventType.DELETED));
                    });

            return deleteLog;
        }

        private void deleteJobFiles(String jobID) {
            // Make sure we do not include this job in the overview
            try {
                Files.deleteIfExists(new File(webOverviewDir, jobID + JSON_FILE_ENDING).toPath());
            } catch (IOException ioe) {
                LOG.warn("Could not delete file from overview directory.", ioe);
            }

            // Clean up job files we may have created
            File jobDirectory = new File(webJobDir, jobID);
            try {
                FileUtils.deleteDirectory(jobDirectory);
            } catch (IOException ioe) {
                LOG.warn("Could not clean up job directory.", ioe);
            }

            try {
                Files.deleteIfExists(new File(webJobDir, jobID + JSON_FILE_ENDING).toPath());
            } catch (IOException ioe) {
                LOG.warn("Could not delete file from job directory.", ioe);
            }
        }
    }

    private static String convertLegacyJobOverview(String legacyOverview) throws IOException {
        JsonNode root = mapper.readTree(legacyOverview);
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
                        numTasks);
        MultipleJobsDetails multipleJobsDetails =
                new MultipleJobsDetails(Collections.singleton(jobDetails));

        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, multipleJobsDetails);
        return sw.toString();
    }

    /**
     * This method replicates the JSON response that would be given by the JobsOverviewHandler when
     * listing both running and finished jobs.
     *
     * <p>Every job archive contains a joboverview.json file containing the same structure. Since
     * jobs are archived on their own however the list of finished jobs only contains a single job.
     *
     * <p>For the display in the HistoryServer WebFrontend we have to combine these overviews.
     */
    private static void updateJobOverview(File webOverviewDir, File webDir) {
        try (JsonGenerator gen =
                jacksonFactory.createGenerator(
                        HistoryServer.createOrGetFile(webDir, JobsOverviewHeaders.URL))) {
            File[] overviews = new File(webOverviewDir.getPath()).listFiles();
            if (overviews != null) {
                Collection<JobDetails> allJobs = new ArrayList<>(overviews.length);
                for (File overview : overviews) {
                    MultipleJobsDetails subJobs =
                            mapper.readValue(overview, MultipleJobsDetails.class);
                    allJobs.addAll(subJobs.getJobs());
                }
                mapper.writeValue(gen, new MultipleJobsDetails(allJobs));
            }
        } catch (IOException ioe) {
            LOG.error("Failed to update job overview.", ioe);
        }
    }
}
