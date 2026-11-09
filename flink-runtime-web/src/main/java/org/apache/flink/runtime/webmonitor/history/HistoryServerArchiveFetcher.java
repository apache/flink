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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.LAZY;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.PENDING;
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
 *
 * @param <Entry> the type of entries returned by the underlying {@link ArchiveStorage}.
 */
public class HistoryServerArchiveFetcher<Entry> implements AutoCloseable {

    /** Possible archive operations in history-server. */
    public enum ArchiveEventType {
        /** Archive is pending to be processed. */
        PENDING,
        /** Overview content is currently parsing. */
        OVERVIEW_PARSING,
        /** Overview content of archive was parsed and created in history server successfully. */
        OVERVIEW_CREATED,
        /** Detail content of archive is currently parsing. */
        DETAIL_PARSING,
        /** Archive was found in one refresh location and created in history server. */
        CREATED,
        /** Archive was deleted from one of refresh locations and deleted from history server. */
        DELETED
    }

    /**
     * Completes when a lazy fetch of one job has finished. It is registered before the archive is
     * read, so concurrent requests for the same job can always wait on it. Cancelling it also
     * cancels the detail task, if one was submitted.
     */
    private static final class FetchFuture extends CompletableFuture<Void> {
        private final CompletableFuture<Void> overviewReady = new CompletableFuture<>();
        private volatile Future<?> detailTask;

        void setDetailTask(Future<?> detailTask) {
            this.detailTask = detailTask;
        }

        boolean isDetailTaskSubmitted() {
            return detailTask != null;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            Future<?> task = detailTask;
            if (task != null) {
                task.cancel(mayInterruptIfRunning);
            }
            overviewReady.cancel(mayInterruptIfRunning);
            return super.cancel(mayInterruptIfRunning);
        }
    }

    /** Lock of a single job, removed from the map when nobody holds or waits for it. */
    private static final class JobLock {
        private final ReentrantLock lock = new ReentrantLock();
        // guarded by the compute calls on the jobLocks map
        private int users;
    }

    /** A held job lock that does not throw on close. */
    private interface HeldJobLock extends AutoCloseable {
        @Override
        void close();
    }

    private HeldJobLock lockJob(String jobId) {
        JobLock jobLock =
                jobLocks.compute(
                        jobId,
                        (id, existing) -> {
                            JobLock result = existing != null ? existing : new JobLock();
                            result.users++;
                            return result;
                        });
        jobLock.lock.lock();
        return () -> {
            jobLock.lock.unlock();
            jobLocks.compute(jobId, (id, existing) -> --existing.users == 0 ? null : existing);
        };
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
    protected static final String JOBS_KEY_PREFIX = JOBS_SUBDIR + "/";
    protected static final String JOB_OVERVIEWS_KEY_PREFIX = JOB_OVERVIEWS_SUBDIR + "/";

    protected final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    protected final List<HistoryServer.RefreshLocation> refreshDirs;
    protected final Consumer<ArchiveEvent> archiveEventListener;
    protected final boolean processExpiredArchiveDeletion;
    protected final ArchiveRetainedStrategy retainedStrategy;

    /** Cache of all available archives identified by their id. */
    protected final Map<Path, Set<String>> cachedArchivesPerRefreshDirectory;

    protected final ArchiveStorage<Entry> archiveStorage;

    /** Executor for loading archives. */
    private final ExecutorService commonFetchExecutor;

    private final ExecutorService individualFetchExecutor;
    private final Map<String, FetchFuture> commonFetchTasks;
    private final Map<String, FetchFuture> individualFetchTasks;
    private final ConcurrentHashMap<String, ArchiveMetaInfo> archiveMetaInfoCache;

    /** Max number of latest archives cached locally, or -1 if all retained ones are cached. */
    private final int cachedRetainedJobs;

    private final int numCachedMostRecentlyViewedJobs;

    /**
     * Guards the bookkeeping of the locally cached jobs (the most recently viewed ones). It is only
     * held for in-memory updates, never while reading or deleting archives, and never while waiting
     * for a job lock or a fetch.
     */
    private final Object localStateLock = new Object();

    /**
     * Serializes the fetch, the deletion and the pinning of a single job. A job lock may be held
     * during I/O. The lock order is job lock, then {@link #localStateLock}, and at most one job
     * lock is held at a time.
     */
    private final ConcurrentHashMap<String, JobLock> jobLocks = new ConcurrentHashMap<>();

    /** Serializes the rebuilds of the combined overviews. It is a leaf lock. */
    private final Object overviewLock = new Object();

    /** Number of requests currently serving a job, only incremented while holding its job lock. */
    private final ConcurrentHashMap<String, Integer> jobsInUse = new ConcurrentHashMap<>();

    /** Access ordered, the eldest entry is the least recently viewed job. */
    private final Map<String, Boolean> mostRecentlyViewedJobs =
            new LinkedHashMap<>(16, 0.75f, true);

    HistoryServerArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ArchiveEvent> archiveEventListener,
            boolean cleanupExpiredArchives,
            ArchiveRetainedStrategy retainedStrategy,
            ArchiveStorage<Entry> archiveStorage,
            ConcurrentHashMap<String, ArchiveMetaInfo> archiveMetaInfoCache,
            int lazyFetchExecutorCommonPoolSize,
            int lazyFetchExecutorIndividualPoolSize)
            throws IOException {
        this(
                refreshDirs,
                webDir,
                archiveEventListener,
                cleanupExpiredArchives,
                retainedStrategy,
                archiveStorage,
                archiveMetaInfoCache,
                lazyFetchExecutorCommonPoolSize,
                lazyFetchExecutorIndividualPoolSize,
                -1,
                1);
    }

    HistoryServerArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ArchiveEvent> archiveEventListener,
            boolean cleanupExpiredArchives,
            ArchiveRetainedStrategy retainedStrategy,
            ArchiveStorage<Entry> archiveStorage,
            ConcurrentHashMap<String, ArchiveMetaInfo> archiveMetaInfoCache,
            int lazyFetchExecutorCommonPoolSize,
            int lazyFetchExecutorIndividualPoolSize,
            int cachedRetainedJobs,
            int numCachedMostRecentlyViewedJobs)
            throws IOException {
        this.cachedRetainedJobs = cachedRetainedJobs;
        this.numCachedMostRecentlyViewedJobs = numCachedMostRecentlyViewedJobs;
        this.refreshDirs = checkNotNull(refreshDirs);
        this.archiveEventListener = archiveEventListener;
        this.processExpiredArchiveDeletion = cleanupExpiredArchives;
        this.retainedStrategy = checkNotNull(retainedStrategy);
        this.cachedArchivesPerRefreshDirectory = new HashMap<>();
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedArchivesPerRefreshDirectory.put(
                    refreshDir.getPath(), ConcurrentHashMap.newKeySet());
        }
        checkNotNull(webDir);
        this.archiveStorage = archiveStorage;
        this.archiveMetaInfoCache = archiveMetaInfoCache;
        this.commonFetchExecutor =
                Executors.newFixedThreadPool(
                        lazyFetchExecutorCommonPoolSize,
                        new ExecutorThreadFactory("HistoryServer-commonFetchExecutor"));
        this.individualFetchExecutor =
                Executors.newFixedThreadPool(
                        lazyFetchExecutorIndividualPoolSize,
                        new ExecutorThreadFactory("HistoryServer-individualFetchExecutor"));
        this.commonFetchTasks = new ConcurrentHashMap<>();
        this.individualFetchTasks = new ConcurrentHashMap<>();
        updateJobOverview();

        if (LOG.isInfoEnabled()) {
            for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
                LOG.info("Monitoring directory {} for archives.", refreshDir.getPath());
            }
        }
    }

    void fetchArchives(HistoryServerOptions.HistoryServerArchiveLoadMode archiveLoadMode) {
        LOG.debug("Starting archive fetching.");
        scanArchives(archiveLoadMode, true);
    }

    void scanArchives(
            HistoryServerOptions.HistoryServerArchiveLoadMode archiveLoadMode, boolean fetch) {
        LOG.debug("Starting archive fetching.");
        try {
            List<ArchiveEvent> events = new ArrayList<>();
            Map<Path, Set<String>> archivesToRemove = new HashMap<>();
            cachedArchivesPerRefreshDirectory.forEach(
                    (path, archives) -> archivesToRemove.put(path, new HashSet<>(archives)));
            Map<Path, Set<Path>> archivesBeyondRetainedLimit = new HashMap<>();
            Map<Path, Set<String>> archivesToEvictLocally = new HashMap<>();
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

                if (archivesToRemove.containsKey(refreshDir)) {
                    Set<String> listedIds = new HashSet<>();
                    archives.forEach(archive -> listedIds.add(archive.getPath().getName()));
                    synchronized (localStateLock) {
                        for (String viewedId : mostRecentlyViewedJobs.keySet()) {
                            if (!listedIds.contains(viewedId)
                                    && isUnderRefreshDirectory(refreshLocation, viewedId)) {
                                archivesToRemove.get(refreshDir).add(viewedId);
                            }
                        }
                    }
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

                    if (cachedRetainedJobs > 0
                            && fileOrderedIndexOnModifiedTime > cachedRetainedJobs) {
                        // the remote archive stays, only evict what an earlier scan cached
                        if (cachedArchivesPerRefreshDirectory.get(refreshDir).contains(archiveId)) {
                            archivesToEvictLocally
                                    .computeIfAbsent(refreshDir, ignored -> new HashSet<>())
                                    .add(archiveId);
                        }
                        continue;
                    }

                    if (fetch) {
                        try (HeldJobLock ignored = lockJob(archiveId)) {
                            fetchArchive(
                                    refreshDir, archiveId, archivePath, archiveLoadMode, events);
                        }
                    }
                }
            }

            // clean local
            if (archivesToRemove.values().stream().flatMap(Set::stream).findAny().isPresent()
                    && processExpiredArchiveDeletion) {
                events.addAll(cleanupExpiredArchives(archivesToRemove));
            }
            // clean remote and local
            if (!archivesBeyondRetainedLimit.isEmpty()) {
                events.addAll(cleanupArchivesBeyondRetainedLimit(archivesBeyondRetainedLimit));
            }
            // clean local only
            if (!archivesToEvictLocally.isEmpty()) {
                synchronized (localStateLock) {
                    archivesToEvictLocally
                            .values()
                            .forEach(ids -> ids.removeAll(mostRecentlyViewedJobs.keySet()));
                }
                events.addAll(cleanupExpiredArchives(archivesToEvictLocally));
            }
            if (!events.isEmpty()) {
                refreshOverview();
            }
            events.forEach(archiveEventListener);
            LOG.debug("Finished archive scan.");
        } catch (Exception e) {
            LOG.error("Critical failure while fetching/processing archives.", e);
        }
    }

    private void fetchArchive(
            Path refreshDir,
            String archiveId,
            Path archivePath,
            HistoryServerOptions.HistoryServerArchiveLoadMode archiveLoadMode,
            List<ArchiveEvent> events)
            throws Exception {
        // checked while holding the job lock, a concurrent scan may have fetched it meanwhile
        if (cachedArchivesPerRefreshDirectory.get(refreshDir).contains(archiveId)) {
            LOG.trace("Ignoring archive {} because it was already fetched.", archivePath);
        } else {
            LOG.info("Processing archive {}.", archivePath);
            try {
                List<ArchiveEvent> processArchiveEvents =
                        LAZY.equals(archiveLoadMode)
                                ? lazyProcessArchive(archiveId, archivePath, refreshDir)
                                : processArchive(archiveId, archivePath, refreshDir);
                events.addAll(processArchiveEvents);
                cachedArchivesPerRefreshDirectory.get(refreshDir).add(archiveId);
                LOG.info("Processing archive {} finished.", archivePath);
            } catch (Exception e) {
                LOG.error("Failure while fetching/processing archive {}.", archiveId, e);
                deleteCachedArchives(archiveId, refreshDir);
            }
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
            throws Exception {
        return Collections.singletonList(processJobArchive(archiveId, archivePath));
    }

    ArchiveEvent processJobArchive(String jobId, Path jobArchive) throws IOException {
        for (ArchivedJson archive : FsJsonArchivist.readArchivedJsons(jobArchive)) {
            String path = archive.getPath();
            String json = archive.getJson();

            String key;
            if (path.equals(JobsOverviewHeaders.URL)) {
                key = JOB_OVERVIEWS_KEY_PREFIX + jobId + JSON_FILE_ENDING;
            } else if (path.equals("/joboverview")) { // legacy path
                LOG.debug("Migrating legacy archive {}", jobArchive);
                json = convertLegacyJobOverview(json);
                key = JOB_OVERVIEWS_KEY_PREFIX + jobId + JSON_FILE_ENDING;
            } else {
                // this implicitly writes into webJobDir; strip the leading '/' from the
                // REST path so that the key is a relative sub-path under the storage root
                key = path.substring(1) + JSON_FILE_ENDING;
            }

            archiveStorage.putArchiveContent(key, json);
        }

        return new ArchiveEvent(jobId, ArchiveEventType.CREATED);
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
                    synchronized (localStateLock) {
                        cachedArchivesPerRefreshDirectory.get(refreshDir).removeAll(archives);
                        mostRecentlyViewedJobs.keySet().removeAll(archives);
                    }
                    archives.forEach(
                            archiveId -> {
                                // the files are deleted outside of the state lock, but not while
                                // the job is being fetched
                                try (HeldJobLock ignored = lockJob(archiveId)) {
                                    cleanUpLazyFetchTask(archiveId);
                                    deleteLog.addAll(deleteCachedArchives(archiveId, refreshDir));
                                }
                            });
                });

        return deleteLog;
    }

    List<ArchiveEvent> deleteCachedArchives(String archiveId, Path refreshDir) {
        LOG.info("Archive directories for job {} is deleted", archiveId);
        return Collections.singletonList(deleteJobFiles(archiveId));
    }

    ArchiveEvent deleteJobFiles(String jobId) {
        // Delete job overview file in overviews directory
        try {
            archiveStorage.delete(JOB_OVERVIEWS_KEY_PREFIX + jobId + JSON_FILE_ENDING);
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from overview directory.", ioe);
        }

        // Delete job details directory in jobs directory, jobs/job-id/
        try {
            archiveStorage.deleteEntriesByPrefix(JOBS_KEY_PREFIX + jobId + "/");
        } catch (IOException ioe) {
            LOG.warn("Could not clean up job directory.", ioe);
        }

        // Delete job overview file in jobs directory, jobs/job-id.json
        try {
            archiveStorage.delete(JOBS_KEY_PREFIX + jobId + JSON_FILE_ENDING);
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from job directory.", ioe);
        }

        archiveMetaInfoCache.remove(jobId);
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
        try {
            Collection<JobDetails> allJobs = new ArrayList<>();
            List<Entry> overviews = archiveStorage.getEntriesByPrefix(JOB_OVERVIEWS_KEY_PREFIX);
            int skipped = 0;
            int vanished = 0;
            for (Entry overview : overviews) {
                try {
                    MultipleJobsDetails subJobs;
                    // We treated File as a special case, mainly as a performance trade-off to
                    // avoid the overhead of loading the archive into string.
                    if (overview instanceof File) {
                        subJobs = mapper.readValue((File) overview, MultipleJobsDetails.class);
                    } else {
                        subJobs =
                                mapper.readValue(
                                        archiveStorage.readArchiveContent(overview),
                                        MultipleJobsDetails.class);
                    }
                    allJobs.addAll(subJobs.getJobs());
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // The job was evicted after the entries were listed; it is gone, not broken.
                    vanished++;
                    LOG.debug("Job overview {} was removed while aggregating.", overview, e);
                } catch (Exception e) {
                    // A single malformed/incompatible archive (e.g. written by a different Flink
                    // version) must not prevent the remaining archives from being aggregated.
                    skipped++;
                    LOG.warn(
                            "Failed to parse job overview from entry {}, skipping it.",
                            overview,
                            e);
                }
            }
            if (skipped > 0) {
                LOG.warn("Skipped {} job overview(s) that could not be parsed.", skipped);
            }
            if (overviews.size() > vanished && allJobs.isEmpty()) {
                // Every entry failed to parse; keep the previously written overview instead of
                // replacing it with an empty one.
                LOG.error(
                        "All {} job overview(s) failed to parse; keeping the last known good "
                                + "combined overview instead of overwriting it.",
                        overviews.size() - vanished);
                return;
            }
            String overviewWithJobs = mapper.writeValueAsString(new MultipleJobsDetails(allJobs));
            archiveStorage.putArchiveContent(
                    JobsOverviewHeaders.URL.substring(1) + JSON_FILE_ENDING, overviewWithJobs);
        } catch (Exception e) {
            LOG.error("Failed to update job overview.", e);
        }
    }

    // -------------------------------- Lazy Load ----------------------------------------
    List<ArchiveEvent> lazyProcessArchive(String archiveId, Path archivePath, Path refreshDir)
            throws Exception {
        return Collections.singletonList(lazyProcessJobArchive(archiveId, archivePath, false));
    }

    ArchiveEvent lazyProcessJobArchive(String jobId, Path jobArchive, boolean individual)
            throws Exception {
        final ArchiveMetaInfo archiveMetaInfo = new ArchiveMetaInfo(jobId, PENDING, jobArchive);
        ArchiveMetaInfo existing = archiveMetaInfoCache.putIfAbsent(jobId, archiveMetaInfo);
        if (existing != null) {
            return new ArchiveEvent(jobId, existing.getEventType());
        }

        ExecutorService fetchExecutor;
        Map<String, FetchFuture> fetchTasks;
        if (individual) {
            fetchExecutor = individualFetchExecutor;
            fetchTasks = individualFetchTasks;
        } else {
            fetchExecutor = commonFetchExecutor;
            fetchTasks = commonFetchTasks;
        }

        // registered before the archive is read, so concurrent requests wait for this fetch
        final FetchFuture fetchFuture = new FetchFuture();
        fetchTasks.put(jobId, fetchFuture);
        try {
            return lazyProcessJobArchive(
                    jobId,
                    jobArchive,
                    individual,
                    archiveMetaInfo,
                    fetchExecutor,
                    fetchTasks,
                    fetchFuture);
        } catch (Throwable t) {
            // do not leave a half processed entry behind, a later request fetches the job again
            archiveMetaInfoCache.remove(jobId, archiveMetaInfo);
            fetchTasks.remove(jobId, fetchFuture);
            fetchFuture.overviewReady.completeExceptionally(t);
            fetchFuture.completeExceptionally(t);
            throw t;
        }
    }

    private ArchiveEvent lazyProcessJobArchive(
            String jobId,
            Path jobArchive,
            boolean individual,
            ArchiveMetaInfo archiveMetaInfo,
            ExecutorService fetchExecutor,
            Map<String, FetchFuture> fetchTasks,
            FetchFuture fetchFuture)
            throws Exception {
        archiveMetaInfo.setEventType(ArchiveEventType.OVERVIEW_PARSING);

        Collection<ArchivedJson> archivedJsons = FsJsonArchivist.readArchivedJsons(jobArchive);
        List<ArchivedJson> detailArchives = new ArrayList<>();
        boolean overviewCreated = false;

        for (ArchivedJson archive : archivedJsons) {
            String path = archive.getPath();
            String json = archive.getJson();

            if (path.equals(JobsOverviewHeaders.URL)) {
                String key = JOB_OVERVIEWS_KEY_PREFIX + jobId + JSON_FILE_ENDING;
                archiveStorage.putArchiveContent(key, json);
                overviewCreated = true;
            } else if (path.equals("/joboverview")) { // legacy path
                LOG.debug("Migrating legacy archive {}", jobArchive);
                json = convertLegacyJobOverview(json);
                String key = JOB_OVERVIEWS_KEY_PREFIX + jobId + JSON_FILE_ENDING;
                archiveStorage.putArchiveContent(key, json);
                overviewCreated = true;
            } else if (path.equals("/jobs/" + jobId)) {
                String key = JOBS_KEY_PREFIX + jobId + JSON_FILE_ENDING;
                archiveStorage.putArchiveContent(key, json);
            } else {
                detailArchives.add(archive);
            }
        }

        if (!overviewCreated && detailArchives.isEmpty()) {
            throw new RuntimeException("Archive of job " + jobId + " is empty");
        }

        fetchFuture.overviewReady.complete(null);
        if (!detailArchives.isEmpty()) {
            fetchFuture.setDetailTask(
                    fetchExecutor.submit(
                            () -> {
                                try {
                                    archiveMetaInfo.setEventType(ArchiveEventType.DETAIL_PARSING);
                                    for (ArchivedJson archive : detailArchives) {
                                        if (Thread.currentThread().isInterrupted()) {
                                            LOG.debug(
                                                    "Detail parsing for job {} was cancelled.",
                                                    jobId);
                                            return;
                                        }
                                        String path = archive.getPath();
                                        String json = archive.getJson();
                                        // this implicitly writes into webJobDir; strip the leading
                                        // '/' from the REST path so that the key is a relative
                                        // sub-path under the storage root
                                        String key = path.substring(1) + JSON_FILE_ENDING;
                                        try {
                                            archiveStorage.putArchiveContent(key, json);
                                        } catch (IOException e) {
                                            LOG.error(
                                                    "Failed to write detail archive file for job {}, path {}.",
                                                    jobId,
                                                    path,
                                                    e);
                                        }
                                    }
                                    archiveMetaInfo.setEventType(ArchiveEventType.CREATED);
                                    if (individual) {
                                        refreshOverview();
                                    }
                                    LOG.debug("Async detail parsing for job {} finished.", jobId);
                                } finally {
                                    fetchTasks.remove(jobId, fetchFuture);
                                    fetchFuture.complete(null);
                                }
                            }));
        }

        ArchiveEventType archiveEventType =
                overviewCreated ? ArchiveEventType.OVERVIEW_CREATED : ArchiveEventType.CREATED;
        if (detailArchives.isEmpty()) {
            // otherwise the async task above is now the sole owner of eventType transitions.
            archiveMetaInfo.setEventType(archiveEventType);
            fetchTasks.remove(jobId, fetchFuture);
            fetchFuture.complete(null);
        }
        return new ArchiveEvent(jobId, archiveEventType);
    }

    void lazyFetchArchiveProactively(String jobId, @Nullable Path archivePath) throws Exception {
        resetWhenTriggerLazyFetch(jobId);

        if (archivePath != null) {
            lazyProcessJobArchive(jobId, archivePath, true);
            return;
        }

        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            archivePath = new Path(refreshDir.getPath(), jobId);
            if (refreshDir.getFs().exists(archivePath)) {
                lazyProcessJobArchive(jobId, archivePath, true);
            }
        }
    }

    /**
     * Fetches the job on demand unless a fetch for it is already running or done. Requests for the
     * same job are serialized, so a concurrent request never cancels the fetch of another one.
     */
    void lazyFetchArchiveIfNeeded(String jobId) throws Exception {
        try (HeldJobLock ignored = lockJob(jobId)) {
            if (!needLazyLoadIndividually(jobId)) {
                return;
            }
            ArchiveMetaInfo metaInfo = archiveMetaInfoCache.get(jobId);
            lazyFetchArchiveProactively(jobId, metaInfo == null ? null : metaInfo.getArchivePath());
        }
    }

    /** Marks the job as recently viewed and evicts the least recently viewed ones locally. */
    void markRecentlyViewed(String jobId) {
        if (cachedRetainedJobs <= 0) {
            return;
        }
        synchronized (localStateLock) {
            if (!isOwnedByRefreshDirectory(jobId) || isCachedByScan(jobId)) {
                return;
            }
            mostRecentlyViewedJobs.put(jobId, Boolean.TRUE);
        }
        evictLeastRecentlyViewed(jobId);
    }

    /**
     * Marks the job as being served to a request. It is not evicted until all such requests have
     * called {@link #releaseJob}. This waits for a fetch or a deletion of the job that is running.
     */
    void acquireJob(String jobId) {
        try (HeldJobLock ignored = lockJob(jobId)) {
            jobsInUse.merge(jobId, 1, Integer::sum);
        }
    }

    void releaseJob(String jobId) {
        jobsInUse.computeIfPresent(jobId, (id, count) -> count > 1 ? count - 1 : null);
        // evictions skipped while the job was in use are caught up on here
        if (cachedRetainedJobs > 0 && !jobsInUse.containsKey(jobId)) {
            evictLeastRecentlyViewed(null);
        }
    }

    /** Evicts the least recently viewed jobs beyond the limit, except for the ones in use. */
    private void evictLeastRecentlyViewed(@Nullable String keepJobId) {
        List<ArchiveEvent> events = new ArrayList<>();
        Set<String> skipped = new HashSet<>();
        String candidate;
        while ((candidate = nextEvictionCandidate(keepJobId, skipped)) != null) {
            if (!evictIfIdle(candidate, events)) {
                skipped.add(candidate);
            }
        }
        if (!events.isEmpty()) {
            refreshOverview();
            events.forEach(archiveEventListener);
        }
    }

    @Nullable
    private String nextEvictionCandidate(@Nullable String keepJobId, Set<String> skipped) {
        synchronized (localStateLock) {
            if (mostRecentlyViewedJobs.size() <= numCachedMostRecentlyViewedJobs) {
                return null;
            }
            // the eldest entry is the least recently viewed job
            for (String jobId : mostRecentlyViewedJobs.keySet()) {
                if (!jobId.equals(keepJobId) && !skipped.contains(jobId) && !isInUse(jobId)) {
                    return jobId;
                }
            }
            return null;
        }
    }

    /** Evicts the job unless it is in use. Returns false if it was skipped for that reason. */
    private boolean evictIfIdle(String jobId, List<ArchiveEvent> events) {
        try (HeldJobLock ignored = lockJob(jobId)) {
            boolean cachedByScan;
            synchronized (localStateLock) {
                // checked again, the job may have been used or evicted before its lock was held
                if (!mostRecentlyViewedJobs.containsKey(jobId)
                        || mostRecentlyViewedJobs.size() <= numCachedMostRecentlyViewedJobs) {
                    return true;
                }
                if (isInUse(jobId)) {
                    return false;
                }
                mostRecentlyViewedJobs.remove(jobId);
                // archives that entered the cached window are managed by the scan
                cachedByScan = isCachedByScan(jobId);
            }
            if (!cachedByScan) {
                events.add(deleteJobFiles(jobId));
            }
            return true;
        }
    }

    private boolean isCachedByScan(String jobId) {
        return cachedArchivesPerRefreshDirectory.values().stream()
                .anyMatch(ids -> ids.contains(jobId));
    }

    private boolean isInUse(String jobId) {
        if (jobsInUse.containsKey(jobId)) {
            return true;
        }
        ArchiveMetaInfo metaInfo = archiveMetaInfoCache.get(jobId);
        if (metaInfo == null) {
            return false;
        }
        switch (metaInfo.getEventType()) {
            case PENDING:
            case OVERVIEW_PARSING:
            case DETAIL_PARSING:
                return true;
            default:
                return false;
        }
    }

    /** Application-owned jobs are nested deeper and managed by the application fetcher. */
    private boolean isOwnedByRefreshDirectory(String jobId) {
        return refreshDirs.stream().anyMatch(dir -> isUnderRefreshDirectory(dir, jobId));
    }

    private boolean isUnderRefreshDirectory(HistoryServer.RefreshLocation dir, String jobId) {
        ArchiveMetaInfo metaInfo = archiveMetaInfoCache.get(jobId);
        if (metaInfo == null || metaInfo.getArchivePath() == null) {
            return false;
        }
        // refresh dirs are kept as configured while archive paths are fully qualified
        Path qualifiedDir = dir.getPath().makeQualified(dir.getFs());
        Path qualifiedParent = metaInfo.getArchivePath().makeQualified(dir.getFs()).getParent();
        return qualifiedDir.equals(qualifiedParent);
    }

    void refreshOverview() {
        synchronized (overviewLock) {
            updateOverview();
        }
    }

    void cleanUpArchives(HistoryServerOptions.HistoryServerArchiveLoadMode archiveLoadMode) {
        LOG.debug("Starting archive cleanup.");
        scanArchives(archiveLoadMode, false);
    }

    boolean needLazyLoadIndividually(String jobId) {
        ArchiveMetaInfo archiveMetaInfo = archiveMetaInfoCache.get(jobId);
        if (archiveMetaInfo == null) {
            return true;
        }

        switch (archiveMetaInfo.getEventType()) {
            case PENDING:
            case OVERVIEW_PARSING:
            case OVERVIEW_CREATED:
                FetchFuture commonFetch = commonFetchTasks.get(jobId);
                // a common fetch still reading the archive is waited for instead
                return commonFetch != null
                        && commonFetch.isDetailTaskSubmitted()
                        && !individualFetchTasks.containsKey(jobId);
            default:
                return false;
        }
    }

    void cleanUpLazyFetchTask(String jobId) {
        FetchFuture commonFetchTask = commonFetchTasks.get(jobId);
        if (commonFetchTask != null) {
            commonFetchTask.cancel(true);
            commonFetchTasks.remove(jobId);
        }
        FetchFuture individualFetchTask = individualFetchTasks.get(jobId);
        if (individualFetchTask != null) {
            individualFetchTask.cancel(true);
            individualFetchTasks.remove(jobId);
        }
    }

    @Override
    public void close() {
        ExecutorUtils.gracefulShutdown(1L, TimeUnit.SECONDS, commonFetchExecutor);
        ExecutorUtils.gracefulShutdown(1L, TimeUnit.SECONDS, individualFetchExecutor);
    }

    @VisibleForTesting
    Future<?> getCommonFetchTask(String jobId) {
        return commonFetchTasks.get(jobId);
    }

    void resetWhenTriggerLazyFetch(String jobId) {
        archiveMetaInfoCache.remove(jobId);
        cleanUpLazyFetchTask(jobId);
    }

    /** Waits until the overview of the job is written, the details may still be loading. */
    void waitLazyFetchOverviewReady(String jobId) throws Exception {
        FetchFuture commonFetchTask = commonFetchTasks.get(jobId);
        if (commonFetchTask != null) {
            awaitUnlessCancelled(commonFetchTask.overviewReady);
        }
        FetchFuture individualFetchTask = individualFetchTasks.get(jobId);
        if (individualFetchTask != null) {
            awaitUnlessCancelled(individualFetchTask.overviewReady);
        }
    }

    void waitLazyFetchArchiveFinished(String jobId) throws Exception {
        FetchFuture commonFetchTask = commonFetchTasks.get(jobId);
        if (commonFetchTask != null) {
            awaitUnlessCancelled(commonFetchTask);
        }
        FetchFuture individualFetchTask = individualFetchTasks.get(jobId);
        if (individualFetchTask != null) {
            awaitUnlessCancelled(individualFetchTask);
        }
    }

    /** A cancelled fetch means the job was evicted, the caller reads the storage and retries. */
    private static void awaitUnlessCancelled(Future<?> future) throws Exception {
        try {
            future.get();
        } catch (CancellationException e) {
            LOG.debug("The fetch was cancelled while waiting for it.", e);
        }
    }

    ArchiveMetaInfo getArchiveMetaInfo(String jobId) {
        return archiveMetaInfoCache.get(jobId);
    }
}
