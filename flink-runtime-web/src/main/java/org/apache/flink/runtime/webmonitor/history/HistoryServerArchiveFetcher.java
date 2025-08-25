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
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is used by the {@link HistoryServer} to fetch the job archives that are located at
 * {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are polled in regular
 * intervals, defined by {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}.
 *
 * <p>The archives are downloaded and expanded into a file structure analog to the REST API.
 *
 * <p>Removes existing archives from these directories and the cache if configured by {@link
 * HistoryServerOptions#HISTORY_SERVER_CLEANUP_EXPIRED_JOBS} or {@link
 * HistoryServerOptions#HISTORY_SERVER_RETAINED_JOBS}.
 */
public class HistoryServerArchiveFetcher {

    /** Possible job archive operations in history-server. */
    public enum ArchiveEventType {
        /** Job archive was found in one refresh location and created in history server. */
        CREATED,
        /** Job archive was deleted from one of cache locations. */
        DELETED,
        /** Job archive was deleted from the remote location. */
        DELETED_FROM_REMOTE
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

    /** Maintains a LRU cache of a given capacity. */
    public class MostRecentlyViewedCache extends LinkedHashMap<String, String> {
        private int maxSize;

        public MostRecentlyViewedCache(int capacity) {
            super(capacity, 0.75f, true);
            this.maxSize = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() > maxSize;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(HistoryServerArchiveFetcher.class);

    private static final JsonFactory jacksonFactory = new JsonFactory();
    private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    private static final String JSON_FILE_ENDING = ".json";

    private final List<HistoryServer.RefreshLocation> refreshDirs;
    private final Consumer<ArchiveEvent> jobArchiveEventListener;
    private final boolean processExpiredArchiveDeletion;
    private final boolean processBeyondLimitArchiveDeletion;
    private final boolean processBeyondLimitLocalCacheDeletion;
    private final int maxHistorySize;

    /**
     * Number of retained jobs in the local cache based on the asynchronous fetch, which is based on
     * most recently run jobs.
     */
    private final int generalCachedJobSize;

    private final boolean remoteFetchEnabled;

    /** Cache of all available jobs identified by their id and refresh directory. */
    private final Map<Path, Set<String>> cachedArchivesPerRefreshDirectory;

    /** Cache of all available jobs by id. */
    private final Set<String> cachedArchives;

    private final MostRecentlyViewedCache mostRecentlyViewedCache;

    private final File webDir;
    private final File webJobDir;
    private final File webOverviewDir;

    public HistoryServerArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ArchiveEvent> jobArchiveEventListener,
            boolean cleanupExpiredArchives,
            int maxHistorySize,
            int generalCachedJobSize,
            boolean remoteFetchEnabled,
            int numCachedMostRecentlyViewedJobs)
            throws IOException {
        this.refreshDirs = checkNotNull(refreshDirs);
        this.jobArchiveEventListener = jobArchiveEventListener;
        this.processExpiredArchiveDeletion = cleanupExpiredArchives;
        this.maxHistorySize = maxHistorySize;
        this.remoteFetchEnabled = remoteFetchEnabled;
        if (this.remoteFetchEnabled) {
            this.mostRecentlyViewedCache =
                    new MostRecentlyViewedCache(numCachedMostRecentlyViewedJobs);
        } else {
            this.mostRecentlyViewedCache = null;
        }
        this.generalCachedJobSize = generalCachedJobSize;
        this.processBeyondLimitArchiveDeletion = this.maxHistorySize > 0;
        this.processBeyondLimitLocalCacheDeletion = generalCachedJobSize > 0;
        this.cachedArchivesPerRefreshDirectory = new HashMap<>();
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedArchivesPerRefreshDirectory.put(refreshDir.getPath(), new HashSet<>());
        }
        this.cachedArchives = new HashSet<>();
        this.webDir = checkNotNull(webDir);
        this.webJobDir = new File(webDir, "jobs");
        Files.createDirectories(webJobDir.toPath());
        this.webOverviewDir = new File(webDir, "overviews");
        Files.createDirectories(webOverviewDir.toPath());
        updateJobOverview(webOverviewDir, webDir);

        if (LOG.isInfoEnabled()) {
            for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
                LOG.info("Monitoring directory {} for archived jobs.", refreshDir.getPath());
            }
        }
    }

    void fetchArchiveByJobId(String jobID) throws IOException {
        if (remoteFetchEnabled) {
            boolean jobIdFound = false;
            if (cachedArchives.contains(jobID)) {
                mostRecentlyViewedCache.put(jobID, jobID);
            } else {
                for (HistoryServer.RefreshLocation refreshLocation : refreshDirs) {
                    Path refreshDir = refreshLocation.getPath();
                    Path jobArchivePath = new Path(refreshDir, jobID);
                    LOG.info(
                            String.format(
                                    "Fetching JobId archive %s from %s.", jobID, jobArchivePath));
                    if (jobArchivePath.getFileSystem().exists(jobArchivePath)) {
                        LOG.info("Processing archive {}.", jobArchivePath);
                        try {
                            processArchive(jobID, jobArchivePath);
                            cachedArchives.add(jobID);
                            cachedArchivesPerRefreshDirectory.get(refreshDir).add(jobID);
                            LOG.info("Processing archive {} finished.", jobArchivePath);
                        } catch (IOException e) {
                            LOG.error(
                                    "Failure while fetching/processing job archive for job {}.",
                                    jobID,
                                    e);
                            deleteJobFiles(jobID);
                            throw e;
                        }
                        jobIdFound = true;
                        mostRecentlyViewedCache.put(jobID, jobID);
                        break;
                    }
                }
                if (!jobIdFound) {
                    LOG.warn("Unable to find archive in remote job archives for job {}.", jobID);
                }
            }
        } else {
            LOG.warn("Unable to fetch jobs from remote archives as this feature is not enabled");
        }
    }

    void fetchArchives() {
        try {
            LOG.debug("Starting archive fetching.");
            List<ArchiveEvent> events = new ArrayList<>();
            Map<Path, Set<String>> expiredJobsToRemove = new HashMap<>();
            cachedArchivesPerRefreshDirectory.forEach(
                    (path, archives) -> expiredJobsToRemove.put(path, new HashSet<>(archives)));

            Set<Path> remoteJobArchivesToRemove = new HashSet<>();
            Map<Path, Set<String>> cachedJobArchivesToRemove = new HashMap<>();

            int generalCachedJobCount = 0;
            for (HistoryServer.RefreshLocation refreshLocation : refreshDirs) {
                Path refreshDir = refreshLocation.getPath();
                LOG.debug("Checking archive directory {}.", refreshDir);

                // contents of /:refreshDir
                FileStatus[] jobArchives;
                try {
                    jobArchives = listArchives(refreshLocation.getFs(), refreshDir);
                } catch (IOException e) {
                    LOG.error("Failed to access job archive location for path {}.", refreshDir, e);
                    // something went wrong, potentially due to a concurrent deletion
                    // do not remove any jobs now; we will retry later
                    expiredJobsToRemove.remove(refreshDir);
                    continue;
                }

                int historySize = 0;
                for (FileStatus jobArchive : jobArchives) {
                    Path jobArchivePath = jobArchive.getPath();
                    String jobID = jobArchivePath.getName();
                    if (!isValidJobID(jobID, refreshDir)) {
                        continue;
                    }

                    expiredJobsToRemove.get(refreshDir).remove(jobID);

                    historySize++;

                    if (!remoteFetchEnabled
                            || (remoteFetchEnabled
                                    && !mostRecentlyViewedCache.containsKey(jobID))) {
                        generalCachedJobCount++;
                    }

                    boolean processJob = true;
                    boolean remoteArchiveDeletion =
                            historySize > maxHistorySize && processBeyondLimitArchiveDeletion;
                    boolean localCacheDeletion =
                            generalCachedJobCount > generalCachedJobSize
                                    && processBeyondLimitLocalCacheDeletion;

                    if (remoteArchiveDeletion) {
                        remoteJobArchivesToRemove.add(jobArchivePath);
                        processJob = false;
                    }

                    if (remoteArchiveDeletion || localCacheDeletion) {
                        if ((!remoteFetchEnabled
                                        || (remoteFetchEnabled
                                                && !mostRecentlyViewedCache.containsKey(jobID)))
                                && cachedArchives.contains(jobID)) {
                            cachedJobArchivesToRemove
                                    .computeIfAbsent(refreshDir, ignored -> new HashSet<>())
                                    .add(jobID);
                        }
                        processJob = false;
                    }

                    if (processJob) {
                        if (cachedArchives.contains(jobID)) {
                            LOG.trace(
                                    "Ignoring archive {} because it was already fetched.",
                                    jobArchivePath);
                        } else {
                            LOG.info("Processing archive {}.", jobArchivePath);
                            try {
                                processArchive(jobID, jobArchivePath);
                                events.add(new ArchiveEvent(jobID, ArchiveEventType.CREATED));
                                cachedArchives.add(jobID);
                                cachedArchivesPerRefreshDirectory.get(refreshDir).add(jobID);
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
            }

            // Remove any expired jobs before cleaning up any other local cached jobs
            if (expiredJobsToRemove.values().stream().flatMap(Set::stream).findAny().isPresent()
                    && processExpiredArchiveDeletion) {
                events.addAll(cleanupCachedJobs(expiredJobsToRemove));
            }

            int numCurrentCachedMostRecentlyViewedJobs =
                    remoteFetchEnabled ? mostRecentlyViewedCache.size() : 0;

            boolean localCacheDeletion =
                    !cachedJobArchivesToRemove.isEmpty()
                            && cachedArchives.size() - numCurrentCachedMostRecentlyViewedJobs
                                    > generalCachedJobSize
                            && processBeyondLimitLocalCacheDeletion;
            boolean remoteArchiveDeletion =
                    !remoteJobArchivesToRemove.isEmpty() && processBeyondLimitArchiveDeletion;

            if (localCacheDeletion || remoteArchiveDeletion) {
                events.addAll(cleanupCachedJobs(cachedJobArchivesToRemove));
            }
            if (remoteArchiveDeletion) {
                events.addAll(cleanupRemoteJobs(remoteJobArchivesToRemove));
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

    private static boolean isValidJobID(String jobId, Path refreshDir) {
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

    private void processArchive(String jobID, Path jobArchive) throws IOException {
        for (ArchivedJson archive : FsJobArchivist.getArchivedJsons(jobArchive)) {
            String path = archive.getPath();
            String json = archive.getJson();

            File target;
            if (path.equals(JobsOverviewHeaders.URL)) {
                target = new File(webOverviewDir, jobID + JSON_FILE_ENDING);
            } else if (path.equals("/joboverview")) { // legacy path
                LOG.debug("Migrating legacy archive {}", jobArchive);
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
    }

    private List<ArchiveEvent> cleanupRemoteJobs(Set<Path> jobArchivesToRemove) {

        List<ArchiveEvent> deleteFromRemoteLog = new ArrayList<>();
        for (Path archive : jobArchivesToRemove) {
            try {
                archive.getFileSystem().delete(archive, false);
                deleteFromRemoteLog.add(
                        new ArchiveEvent(archive.getName(), ArchiveEventType.DELETED_FROM_REMOTE));
            } catch (IOException ioe) {
                LOG.warn("Could not delete old archive " + archive, ioe);
            }
        }
        return deleteFromRemoteLog;
    }

    private List<ArchiveEvent> cleanupCachedJobs(Map<Path, Set<String>> jobsToRemove) {

        List<ArchiveEvent> deleteLog = new ArrayList<>();

        jobsToRemove.forEach(
                (refreshDir, archivesToRemove) -> {
                    for (String jobID : archivesToRemove) {
                        cachedArchivesPerRefreshDirectory.get(refreshDir).remove(jobID);
                        cachedArchives.remove(jobID);
                    }
                });
        jobsToRemove.values().stream()
                .flatMap(Set::stream)
                .forEach(
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
                        numTasks,
                        new HashMap<>());
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
