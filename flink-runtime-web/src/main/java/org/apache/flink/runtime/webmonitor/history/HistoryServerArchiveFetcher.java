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
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.webmonitor.history.retaining.ArchiveRetainedStrategy;
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
import java.util.Collection;
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
 * This class is used by the {@link HistoryServer} to fetch the job archives that are located at
 * {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are polled in regular
 * intervals, defined by {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}.
 *
 * <p>The archives are downloaded and expanded into a file structure analog to the REST API.
 *
 * <p>Removes existing archives from these directories and the cache according to {@link
 * ArchiveRetainedStrategy} and {@link HistoryServerOptions#HISTORY_SERVER_CLEANUP_EXPIRED_JOBS}.
 */
class HistoryServerArchiveFetcher {

    /** Possible archive operations in history-server. */
    public enum ArchiveEventType {
        /** Archive was found in one refresh location and created in history server. */
        CREATED,
        /** Archive was deleted from one of refresh locations and deleted from history server. */
        DELETED
    }

    /** Representation of archive event. */
    public static class ArchiveEvent {
        private final String id;
        private final ArchiveEventType operation;

        ArchiveEvent(String id, ArchiveEventType operation) {
            this.id = id;
            this.operation = operation;
        }

        public String getId() {
            return id;
        }

        public ArchiveEventType getType() {
            return operation;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(HistoryServerArchiveFetcher.class);

    protected static final String JSON_FILE_ENDING = ".json";
    protected static final String JOBS_SUBDIR = "jobs";
    protected static final String JOB_OVERVIEWS_SUBDIR = "overviews";

    protected final JsonFactory jacksonFactory = new JsonFactory();
    protected final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    protected final List<HistoryServer.RefreshLocation> refreshDirs;
    protected final Consumer<ArchiveEvent> archiveEventListener;
    protected final boolean processExpiredArchiveDeletion;
    protected final ArchiveRetainedStrategy retainedStrategy;

    /** Cache of all available archives identified by their id. */
    protected final Map<Path, Set<String>> cachedArchivesPerRefreshDirectory;

    protected final File webDir;
    protected final File webJobDir;
    protected final File webOverviewDir;

    HistoryServerArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ArchiveEvent> archiveEventListener,
            boolean cleanupExpiredArchives,
            ArchiveRetainedStrategy retainedStrategy)
            throws IOException {
        this.refreshDirs = checkNotNull(refreshDirs);
        this.archiveEventListener = archiveEventListener;
        this.processExpiredArchiveDeletion = cleanupExpiredArchives;
        this.retainedStrategy = checkNotNull(retainedStrategy);
        this.cachedArchivesPerRefreshDirectory = new HashMap<>();
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedArchivesPerRefreshDirectory.put(refreshDir.getPath(), new HashSet<>());
        }
        this.webDir = checkNotNull(webDir);
        this.webJobDir = new File(webDir, JOBS_SUBDIR);
        Files.createDirectories(webJobDir.toPath());
        this.webOverviewDir = new File(webDir, JOB_OVERVIEWS_SUBDIR);
        Files.createDirectories(webOverviewDir.toPath());
        updateJobOverview();

        if (LOG.isInfoEnabled()) {
            for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
                LOG.info("Monitoring directory {} for archives.", refreshDir.getPath());
            }
        }
    }

    void fetchArchives() {
        try {
            LOG.debug("Starting archive fetching.");
            List<ArchiveEvent> events = new ArrayList<>();
            Map<Path, Set<String>> archivesToRemove = new HashMap<>();
            cachedArchivesPerRefreshDirectory.forEach(
                    (path, archives) -> archivesToRemove.put(path, new HashSet<>(archives)));
            Map<Path, Set<Path>> archivesBeyondRetainedLimit = new HashMap<>();
            for (HistoryServer.RefreshLocation refreshLocation : refreshDirs) {
                Path refreshDir = refreshLocation.getPath();
                LOG.debug("Checking archive directory {}.", refreshDir);

                List<FileStatus> archives;
                try {
                    archives = listValidArchives(refreshLocation.getFs(), refreshDir);
                    archives.sort(
                            Comparator.comparingLong(FileStatus::getModificationTime).reversed());
                } catch (IOException e) {
                    LOG.error("Failed to access archive location for path {}.", refreshDir, e);
                    // something went wrong, potentially due to a concurrent deletion
                    // do not remove any archives now; we will retry later
                    archivesToRemove.remove(refreshDir);
                    continue;
                }

                int fileOrderedIndexOnModifiedTime = 0;
                for (FileStatus archive : archives) {
                    Path archivePath = archive.getPath();
                    String archiveId = archivePath.getName();
                    archivesToRemove.get(refreshDir).remove(archiveId);

                    fileOrderedIndexOnModifiedTime++;
                    if (!retainedStrategy.shouldRetain(archive, fileOrderedIndexOnModifiedTime)) {
                        archivesBeyondRetainedLimit
                                .computeIfAbsent(refreshDir, ignored -> new HashSet<>())
                                .add(archivePath);
                        continue;
                    }

                    if (cachedArchivesPerRefreshDirectory.get(refreshDir).contains(archiveId)) {
                        LOG.trace(
                                "Ignoring archive {} because it was already fetched.", archivePath);
                    } else {
                        LOG.info("Processing archive {}.", archivePath);
                        try {
                            events.addAll(processArchive(archiveId, archivePath, refreshDir));
                            cachedArchivesPerRefreshDirectory.get(refreshDir).add(archiveId);
                            LOG.info("Processing archive {} finished.", archivePath);
                        } catch (IOException e) {
                            LOG.error(
                                    "Failure while fetching/processing archive {}.", archiveId, e);
                            deleteCachedArchives(archiveId, refreshDir);
                        }
                    }
                }
            }

            if (archivesToRemove.values().stream().flatMap(Set::stream).findAny().isPresent()
                    && processExpiredArchiveDeletion) {
                events.addAll(cleanupExpiredArchives(archivesToRemove));
            }
            if (!archivesBeyondRetainedLimit.isEmpty()) {
                events.addAll(cleanupArchivesBeyondRetainedLimit(archivesBeyondRetainedLimit));
            }
            if (!events.isEmpty()) {
                updateOverview();
            }
            events.forEach(archiveEventListener);
            LOG.debug("Finished archive fetching.");
        } catch (Exception e) {
            LOG.error("Critical failure while fetching/processing archives.", e);
        }
    }

    List<FileStatus> listValidArchives(FileSystem refreshFS, Path refreshDir) throws IOException {
        return listValidJobArchives(refreshFS, refreshDir);
    }

    List<FileStatus> listValidJobArchives(FileSystem refreshFS, Path refreshDir)
            throws IOException {
        List<FileStatus> jobArchives = new ArrayList<>();
        // contents of /:refreshDir
        FileStatus[] archives = refreshFS.listStatus(refreshDir);
        if (archives == null) {
            // the entire refreshDirectory was removed
            return jobArchives;
        }

        // Check for job archive files located directly in the refresh directory and named according
        // to the job ID format
        for (FileStatus archive : archives) {
            if (!archive.isDir() && isValidJobId(archive.getPath().getName(), refreshDir)) {
                jobArchives.add(archive);
            }
        }

        return jobArchives;
    }

    boolean isValidJobId(String jobId, Path refreshDir) {
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

    List<ArchiveEvent> processArchive(String archiveId, Path archivePath, Path refreshDir)
            throws IOException {
        return Collections.singletonList(processJobArchive(archiveId, archivePath));
    }

    ArchiveEvent processJobArchive(String jobId, Path jobArchive) throws IOException {
        for (ArchivedJson archive : FsJsonArchivist.readArchivedJsons(jobArchive)) {
            String path = archive.getPath();
            String json = archive.getJson();

            File target;
            if (path.equals(JobsOverviewHeaders.URL)) {
                target = new File(webOverviewDir, jobId + JSON_FILE_ENDING);
            } else if (path.equals("/joboverview")) { // legacy path
                LOG.debug("Migrating legacy archive {}", jobArchive);
                json = convertLegacyJobOverview(json);
                target = new File(webOverviewDir, jobId + JSON_FILE_ENDING);
            } else {
                // this implicitly writes into webJobDir
                target = new File(webDir, path + JSON_FILE_ENDING);
            }

            writeTargetFile(target, json);
        }

        return new ArchiveEvent(jobId, ArchiveEventType.CREATED);
    }

    void writeTargetFile(File target, String json) throws IOException {
        java.nio.file.Path parent = target.getParentFile().toPath();

        try {
            Files.createDirectories(parent);
        } catch (FileAlreadyExistsException ignored) {
            // there may be left-over directories from the previous attempt
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

    List<ArchiveEvent> cleanupArchivesBeyondRetainedLimit(Map<Path, Set<Path>> archivesToRemove) {
        Map<Path, Set<String>> allArchiveIdsToRemove = new HashMap<>();

        for (Map.Entry<Path, Set<Path>> pathSetEntry : archivesToRemove.entrySet()) {
            HashSet<String> archiveIdsToRemove = new HashSet<>();

            for (Path archive : pathSetEntry.getValue()) {
                archiveIdsToRemove.add(archive.getName());
                try {
                    deleteFromRemote(archive);
                } catch (IOException ioe) {
                    LOG.warn("Could not delete old archive {}", archive, ioe);
                }
            }
            allArchiveIdsToRemove.put(pathSetEntry.getKey(), archiveIdsToRemove);
        }

        return cleanupExpiredArchives(allArchiveIdsToRemove);
    }

    void deleteFromRemote(Path archive) throws IOException {
        archive.getFileSystem().delete(archive, false);
    }

    List<ArchiveEvent> cleanupExpiredArchives(Map<Path, Set<String>> archivesToRemove) {
        List<ArchiveEvent> deleteLog = new ArrayList<>();

        archivesToRemove.forEach(
                (refreshDir, archives) -> {
                    cachedArchivesPerRefreshDirectory.get(refreshDir).removeAll(archives);
                    archives.forEach(
                            archiveId ->
                                    deleteLog.addAll(deleteCachedArchives(archiveId, refreshDir)));
                });

        return deleteLog;
    }

    List<ArchiveEvent> deleteCachedArchives(String archiveId, Path refreshDir) {
        LOG.info("Archive directories for job {} is deleted", archiveId);
        return Collections.singletonList(deleteJobFiles(archiveId));
    }

    ArchiveEvent deleteJobFiles(String jobId) {
        // Make sure we do not include this job in the overview
        try {
            Files.deleteIfExists(new File(webOverviewDir, jobId + JSON_FILE_ENDING).toPath());
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from overview directory.", ioe);
        }

        // Clean up job files we may have created
        File jobDirectory = new File(webJobDir, jobId);
        try {
            FileUtils.deleteDirectory(jobDirectory);
        } catch (IOException ioe) {
            LOG.warn("Could not clean up job directory.", ioe);
        }

        try {
            Files.deleteIfExists(new File(webJobDir, jobId + JSON_FILE_ENDING).toPath());
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from job directory.", ioe);
        }

        return new ArchiveEvent(jobId, ArchiveEventType.DELETED);
    }

    private String convertLegacyJobOverview(String legacyOverview) throws IOException {
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

    void updateOverview() {
        updateJobOverview();
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
    void updateJobOverview() {
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
