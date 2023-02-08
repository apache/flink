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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is used by the {@link HistoryServer} to process job archives and manage unzipped job
 * files.
 *
 * <p>HistoryServerArchiveProcessor manages job archives and unzipped job files
 *
 * <p>Related local dirs which store job files(local cache):
 *
 * <p>Archived job files - {@link HistoryServerArchiveProcessor#webArchivedDir} stores job archives.
 * One file per job.
 *
 * <p>Unzipped job files - {@link HistoryServerArchiveProcessor#webJobDir} stores unzipped job files
 * - {@link HistoryServerArchiveProcessor#webOverviewDir} stores unzipped job files related to
 * overview
 *
 * <p>Here are some main features:
 *
 * <p>1. Fetch the job archives that are located at {@link
 * HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are polled in regular
 * intervals, defined by {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}. Those
 * job archives are saved in {@link HistoryServerArchiveProcessor#webArchivedDir}
 *
 * <p>2. Unzip specific job archive when user visit those jobs on WebUI of Flink HistoryServer. It
 * saves those job files into {@link HistoryServerArchiveProcessor#webJobDir} and {@link
 * HistoryServerArchiveProcessor#webOverviewDir}
 *
 * <p>The archives are downloaded and unzipped into a file structure analog to the REST API.
 *
 * <p>It provides some cleanup strategies:
 *
 * <p>1. Removes existing archives from these {@link HistoryServerArchiveProcessor#refreshDirs}, and
 * delete `Archived job files` and `Unzipped job files` from the local dirs if configured by {@link
 * HistoryServerOptions#HISTORY_SERVER_RETAINED_JOBS}.
 *
 * <p>2. Removes the local cache(including `Archived job files` and `Unzipped job files`) if
 * configured by {@link HistoryServerOptions#HISTORY_SERVER_CLEANUP_EXPIRED_JOBS} when job archives
 * have been removed from {@link HistoryServerArchiveProcessor#refreshDirs}
 *
 * <p>3. Removes `unzipped job files` in {@link HistoryServerArchiveProcessor#webJobDir} and {@link
 * HistoryServerArchiveProcessor#webOverviewDir} when unzipped job files exceed cache strategies
 * configured by {@link HistoryServerOptions#HISTORY_SERVER_UNZIPPED_JOBS_MAX}.
 */
class HistoryServerArchiveProcessor {

    /** Possible job file operations in history-server. */
    public enum ProcessEventType {
        /** Archived job file was found in one refresh location and downloaded in history server. */
        DOWNLOADED,
        /**
         * HistoryServer reloads Unzipped Job files left by last HistoryServer when it starting .
         */
        RELOADED,
        /** Archived job file was unzipped and Unzipped Job files was created in history server. */
        UNZIPPED,
        /**
         * Delete unzipped Job files which is stored in {@link
         * HistoryServerArchiveProcessor#webJobDir} and {@link
         * HistoryServerArchiveProcessor#webOverviewDir}.
         */
        CLEANED,
        /** Archived job file was deleted from history server. */
        DELETED,
    }

    /** Representation of job process event. */
    public static class ProcessEvent {

        private final String jobID;
        private final ProcessEventType operation;

        ProcessEvent(String jobID, ProcessEventType operation) {
            this.jobID = jobID;
            this.operation = operation;
        }

        public String getJobID() {
            return jobID;
        }

        public ProcessEventType getType() {
            return operation;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(HistoryServerArchiveProcessor.class);
    private static final JsonFactory jacksonFactory = new JsonFactory();
    private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    private static final String JSON_FILE_ENDING = ".json";

    private final List<HistoryServer.RefreshLocation> refreshDirs;
    private final List<ProcessEvent> events;
    private final Consumer<ProcessEvent> jobProcessEventListener;
    private final boolean processExpiredArchiveDeletion;
    private final boolean processBeyondLimitArchiveDeletion;
    private final int maxHistorySize;

    /** Cache of all available jobs identified by their id. */
    private final Map<Path, Set<String>> cachedArchivesPerRefreshDirectory;
    /** Cache of all unzipped jobs. key: jobID */
    private final LoadingCache<String, Boolean> unzippedJobCache;

    /**
     * Root dir for history server REST API. Configured in {@link
     * HistoryServerOptions#HISTORY_SERVER_WEB_DIR} *
     */
    private final File webDir;
    /** This dir stores unzipped job files * */
    private final File webJobDir;
    /**
     * This dir stores job archives fetched from {@link
     * HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. *
     */
    private final File webArchivedDir;
    /** This dir stores unzipped job files related to overview after unzipping job archive* */
    private final File webOverviewDir;

    HistoryServerArchiveProcessor(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ProcessEvent> jobProcessEventListener,
            boolean cleanupExpiredArchives,
            int maxHistorySize,
            int maxUnzippedJobSize)
            throws IOException {
        this.refreshDirs = checkNotNull(refreshDirs);
        this.events = Collections.synchronizedList(new ArrayList<>());
        this.jobProcessEventListener = jobProcessEventListener;
        this.processExpiredArchiveDeletion = cleanupExpiredArchives;
        this.maxHistorySize = maxHistorySize;
        this.processBeyondLimitArchiveDeletion = this.maxHistorySize > 0;
        this.cachedArchivesPerRefreshDirectory = new HashMap<>();
        this.unzippedJobCache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(10)
                        .initialCapacity(10)
                        .maximumSize(maxUnzippedJobSize)
                        .expireAfterAccess(7L, TimeUnit.DAYS)
                        .removalListener(
                                notification -> {
                                    LOG.info(
                                            "Job:{} is removed from cache with reason [{}]",
                                            notification.getKey(),
                                            notification.getCause());
                                    deleteJobFiles((String) notification.getKey());
                                })
                        .build(
                                new CacheLoader<String, Boolean>() {
                                    @Override
                                    public Boolean load(String s) throws IOException {
                                        return unzipArchive(s);
                                    }
                                });
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedArchivesPerRefreshDirectory.put(refreshDir.getPath(), new HashSet<>());
        }
        this.webDir = checkNotNull(webDir);
        this.webArchivedDir = new File(webDir, "archivedJobs");
        Files.createDirectories(webArchivedDir.toPath());
        this.webJobDir = new File(webDir, "jobs");
        Files.createDirectories(webJobDir.toPath());
        this.webOverviewDir = new File(webDir, "overviews");
        Files.createDirectories(webOverviewDir.toPath());
        updateJobOverview(webOverviewDir, webDir);
        initJobCache();

        if (LOG.isInfoEnabled()) {
            for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
                LOG.info("Monitoring directory {} for archived jobs.", refreshDir.getPath());
            }
        }
    }

    /**
     * Reload job archives and Unzipped Job files left by last HistoryServer when HistoryServer
     * starting.
     */
    private void initJobCache() {
        initArchivedJobCache();
        initUnzippedJobCache();
    }

    /** Reload job archives stored in {@link HistoryServerArchiveProcessor#webArchivedDir} */
    private void initArchivedJobCache() {
        if (this.webArchivedDir.list() == null) {
            LOG.info("No legacy archived jobs");
            return;
        }
        Set<String> jobInLocal =
                Arrays.stream(this.webArchivedDir.list()).collect(Collectors.toSet());
        LOG.info("Reload left archived jobs : [{}]", String.join(",", jobInLocal));

        for (HistoryServer.RefreshLocation refreshLocation : refreshDirs) {
            Path refreshDir = refreshLocation.getPath();
            try {
                FileStatus[] jobArchives = listArchives(refreshLocation.getFs(), refreshDir);
                Set<String> jobInRefreshLocation =
                        Arrays.stream(jobArchives)
                                .map(FileStatus::getPath)
                                .map(Path::getName)
                                .collect(Collectors.toSet());
                jobInRefreshLocation.retainAll(jobInLocal);
                this.cachedArchivesPerRefreshDirectory.get(refreshDir).addAll(jobInRefreshLocation);
            } catch (IOException e) {
                LOG.error("Failed to reload archivedJobs in {}.", refreshDir, refreshDir, e);
            }
        }

        for (String jobId : Objects.requireNonNull(this.webArchivedDir.list())) {
            this.cachedArchivesPerRefreshDirectory.forEach((path, archives) -> archives.add(jobId));
        }
    }

    /** Reload Unzipped Job files stored in {@link HistoryServerArchiveProcessor#webOverviewDir} */
    private void initUnzippedJobCache() {
        if (this.webOverviewDir.list() == null) {
            LOG.info("No legacy unzipped jobs");
            return;
        }
        Arrays.stream(Objects.requireNonNull(this.webOverviewDir.list()))
                .filter(
                        jobOverviewJsonFileName ->
                                !StringUtils.isNullOrWhitespaceOnly(jobOverviewJsonFileName)
                                        && jobOverviewJsonFileName.endsWith(".json"))
                .map(this::extractJobID)
                .filter(this::unzippedJobExist)
                .forEach(
                        jobID -> {
                            unzippedJobCache.put(jobID, true);
                            events.add(new ProcessEvent(jobID, ProcessEventType.RELOADED));
                            LOG.info("Reload left unzipped job : {}", jobID);
                        });
    }

    private String extractJobID(String jobOverviewJsonFileName) {
        return jobOverviewJsonFileName.split("\\.")[0];
    }

    /**
     * This method fetch new job archives from `refreshLocation`
     *
     * <p>This method is called in {@link HistoryServer#fetcherExecutor}
     */
    void fetchArchives() {
        try {
            LOG.debug("Starting archive fetching.");
            Map<Path, Set<String>> jobsToRemove = new HashMap<>();
            cachedArchivesPerRefreshDirectory.forEach(
                    (path, archives) -> jobsToRemove.put(path, new HashSet<>(archives)));
            Map<Path, Set<Path>> archivesBeyondSizeLimit = new HashMap<>();
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
                    jobsToRemove.remove(refreshDir);
                    continue;
                }

                int historySize = 0;
                for (FileStatus jobArchive : jobArchives) {
                    Path jobArchivePath = jobArchive.getPath();
                    String jobID = jobArchivePath.getName();
                    if (!isValidJobID(jobID, refreshDir)) {
                        continue;
                    }

                    jobsToRemove.get(refreshDir).remove(jobID);

                    historySize++;
                    if (historySize > maxHistorySize && processBeyondLimitArchiveDeletion) {
                        archivesBeyondSizeLimit
                                .computeIfAbsent(refreshDir, ignored -> new HashSet<>())
                                .add(jobArchivePath);
                        continue;
                    }

                    if (cachedArchivesPerRefreshDirectory.get(refreshDir).contains(jobID)) {
                        LOG.trace(
                                "Ignoring archive {} because it was already fetched.",
                                jobArchivePath);
                    } else {
                        LOG.info("Downloading archive {}.", jobArchivePath);
                        try {
                            downloadArchivedJobToLocal(
                                    new File(this.webArchivedDir, jobID), jobArchivePath);
                            events.add(new ProcessEvent(jobID, ProcessEventType.DOWNLOADED));
                            cachedArchivesPerRefreshDirectory.get(refreshDir).add(jobID);
                            LOG.info("Downloading archive {} finished.", jobArchivePath);
                        } catch (IOException e) {
                            LOG.error(
                                    "Failure while fetching/processing job archive for job {}.",
                                    jobID,
                                    e);
                            deleteArchivedJob(jobID);
                        }
                    }
                }
            }

            if (jobsToRemove.values().stream().flatMap(Set::stream).findAny().isPresent()
                    && processExpiredArchiveDeletion) {
                events.addAll(cleanupExpiredJobs(jobsToRemove));
            }
            if (!archivesBeyondSizeLimit.isEmpty() && processBeyondLimitArchiveDeletion) {
                events.addAll(cleanupJobsBeyondSizeLimit(archivesBeyondSizeLimit));
            }
            events.forEach(jobProcessEventListener::accept);
            events.clear();
            LOG.debug("Finished archive fetching.");
        } catch (Exception e) {
            LOG.error("Critical failure while fetching/processing job archives.", e);
        }
    }

    public Boolean getUnzippedJob(String jobId) {
        try {
            return this.unzippedJobCache.get(jobId);
        } catch (ExecutionException e) {
            LOG.error("Fail to get/unzip archived job: {}", jobId, e);
        }
        return false;
    }

    /**
     * This method unzip specific job archives and save those job files into {@link
     * HistoryServerOptions#HISTORY_SERVER_WEB_DIR}
     *
     * <p>This method is called in {@link HistoryServer#unzipExecutor}
     */
    private Boolean unzipArchive(String jobID) {
        if (unzippedJobExist(jobID)) {
            LOG.debug("Unzipped job: {} exists.", jobID);
            return true;
        } else {
            deleteJobFiles(jobID);
            LOG.info("Unzipping job: {}.", jobID);
            try {
                processArchive(jobID, new Path(new File(webArchivedDir, jobID).toURI()));
            } catch (IOException e) {
                LOG.error("Fail to process archived job: {}", jobID, e);
                return false;
            }
            updateJobOverview(webOverviewDir, webDir);
            LOG.info("Unzip archived job: {} finished", jobID);
            return true;
        }
    }

    private static FileStatus[] listArchives(FileSystem refreshFS, Path refreshDir)
            throws IOException {
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

    private void downloadArchivedJobToLocal(File archivedJob, Path file) throws IOException {
        if (!archivedJob.exists()) {
            try (FSDataInputStream input = file.getFileSystem().open(file);
                    FileOutputStream output = new FileOutputStream(archivedJob)) {
                IOUtils.copyBytes(input, output);
            }
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
        events.add(new ProcessEvent(jobID, ProcessEventType.UNZIPPED));
    }

    private List<ProcessEvent> cleanupJobsBeyondSizeLimit(
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

    private List<ProcessEvent> cleanupExpiredJobs(Map<Path, Set<String>> jobsToRemove) {

        List<ProcessEvent> deleteLog = new ArrayList<>();
        LOG.info(
                "Unzipped job files and Archived Job file for jobs {} were deleted.", jobsToRemove);

        jobsToRemove.forEach(
                (refreshDir, archivesToRemove) -> {
                    cachedArchivesPerRefreshDirectory.get(refreshDir).removeAll(archivesToRemove);
                });
        jobsToRemove.values().stream()
                .flatMap(Set::stream)
                .forEach(
                        removedJobID -> {
                            deleteJobFiles(removedJobID);
                            deleteArchivedJob(removedJobID);
                        });

        return deleteLog;
    }

    private void deleteArchivedJob(String jobID) {
        // Make sure we do not include this job in the overview
        try {
            Files.deleteIfExists(new File(webArchivedDir, jobID).toPath());
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from overview directory.", ioe);
        }

        events.add(new ProcessEvent(jobID, ProcessEventType.DELETED));
    }

    private void deleteJobFiles(String jobID) {
        // Make sure we do not include this job in the overview
        LOG.info("Try to delete unzipped job [{}] files", jobID);
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
        LOG.info("Deleted unzipped job [{}] files", jobID);
        events.add(new ProcessEvent(jobID, ProcessEventType.CLEANED));
    }

    private boolean unzippedJobExist(String jobID) {
        if (Files.notExists(new File(webOverviewDir, jobID + JSON_FILE_ENDING).toPath())
                || Files.notExists(new File(webJobDir, jobID).toPath())
                || Files.notExists(new File(webJobDir, jobID + JSON_FILE_ENDING).toPath())) {
            return false;
        }
        return true;
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
