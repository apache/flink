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

import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.webmonitor.history.kvstore.HistoryServerRocksDBKVStore;
import org.apache.flink.runtime.webmonitor.history.kvstore.KVStore;

import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This class is used by the {@link HistoryServer} to fetch the job archives that are located at
 * {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are polled in regular
 * intervals, defined by {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}.
 *
 * <p>The archives are downloaded, processed and saved into a RocksDB based KVStore analog to the
 * REST API.
 *
 * <p>Removes existing archives from these directories and the cache if configured by {@link
 * HistoryServerOptions#HISTORY_SERVER_CLEANUP_EXPIRED_JOBS} or {@link
 * HistoryServerOptions#HISTORY_SERVER_RETAINED_JOBS}.
 */
public class HistoryServerKVStoreArchiveFetcher extends ArchiveFetcher {

    private final KVStore<String, String> kvStore;

    HistoryServerKVStoreArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File dbPath,
            Consumer<ArchiveEvent> jobArchiveEventListener,
            boolean cleanupExpiredArchives,
            int maxHistorySize)
            throws RocksDBException {
        super(refreshDirs, jobArchiveEventListener, cleanupExpiredArchives, maxHistorySize);
        this.kvStore = new HistoryServerRocksDBKVStore(dbPath);

        updateJobOverview(kvStore);

        if (LOG.isInfoEnabled()) {
            for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
                LOG.info("Monitoring directory {} for archived jobs.", refreshDir.getPath());
            }
        }
    }

    @Override
    void fetchArchives() {
        try {
            LOG.debug("Starting archive fetching.");
            List<ArchiveEvent> events = new ArrayList<>();
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
                        LOG.info("Processing archive {}.", jobArchivePath);
                        try {
                            processArchive(jobID, jobArchivePath);
                            events.add(new ArchiveEvent(jobID, ArchiveEventType.CREATED));
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

            if (jobsToRemove.values().stream().flatMap(Set::stream).findAny().isPresent()
                    && processExpiredArchiveDeletion) {
                events.addAll(cleanupExpiredJobs(jobsToRemove));
            }
            if (!archivesBeyondSizeLimit.isEmpty() && processBeyondLimitArchiveDeletion) {
                events.addAll(cleanupJobsBeyondSizeLimit(archivesBeyondSizeLimit));
            }
            if (!events.isEmpty()) {
                updateJobOverview(kvStore);
            }
            events.forEach(jobArchiveEventListener::accept);
            LOG.debug("Finished archive fetching.");
        } catch (Exception e) {
            LOG.error("Critical failure while fetching/processing job archives.", e);
        }
    }

    /** Processes a job archive, extracting and saving the relevant JSON data to rocksDB. */
    @Override
    void processArchive(String jobID, Path jobArchive) throws IOException {
        // Create a map to store the key-value pairs for batch writing
        Map<String, String> batchEntries = new HashMap<>();

        // Iterate over the archived JSON objects and prepare them for batch write
        for (ArchivedJson archive : FsJobArchivist.getArchivedJsons(jobArchive)) {
            String path = archive.getPath();
            String json = archive.getJson();

            // Special Handling for /job/overview path, save separately in RocksDB
            if (path.equals("/jobs/overview")) {
                path = "/jobs/overview/" + jobID;
            } else if (path.equals("/joboverview")) {
                LOG.debug("Migrating legacy archive {}", jobArchive);
                json = convertLegacyJobOverview(json);
                path = "/jobs/overview/" + jobID;
            }

            // Add the key-value pair to the map for batch write
            batchEntries.put(path, json);
        }

        // Perform batch write to RocksDB using the writeAll method
        try {
            kvStore.writeAll(batchEntries); // Use the batch write function here
        } catch (Exception e) {
            LOG.error("Error saving archive to RocksDB for job {}: {}", jobID, e.getMessage());
            throw new IOException("Error saving archive to RocksDB", e);
        }
    }

    /** Deletes all files associated with a given job ID from the KVStore. */
    @Override
    void deleteJobFiles(String jobID) {
        try {
            // Delete all keys with the prefix "/jobs/{jobID}"
            kvStore.getAllByPrefix("/jobs/" + jobID)
                    .forEach(
                            key -> {
                                try {
                                    kvStore.delete(key);
                                } catch (Exception e) {
                                    LOG.warn("Could not delete key {} from RocksDB.", key, e);
                                }
                            });

            // Explicitly delete the job overview key "/jobs/overview/{jobID}"
            String overviewKey = "/jobs/overview/" + jobID;
            try {
                kvStore.delete(overviewKey);
            } catch (Exception e) {
                LOG.warn("Could not delete overview key {} from RocksDB.", overviewKey, e);
            }
        } catch (Exception e) {
            LOG.warn("Could not delete job {} from RocksDB.", jobID, e);
        }
    }

    /**
     * This method replicates the JSON response that would be given by the JobsOverviewHandler when
     * listing both running and finished jobs.
     *
     * <p>Every job archive in RocksDB contains a job overview with a similar structure. Since jobs
     * are archived individually, the list of finished jobs may contain only a single job per
     * record.
     *
     * <p>For display in the HistoryServer WebFrontend, we need to combine these individual job
     * overviews into a consolidated view. This method retrieves all job overview entries from
     * RocksDB, merges them into a single JSON response, and can store the combined result back in
     * RocksDB.
     */
    static void updateJobOverview(KVStore<String, String> kvStore) {
        try {
            Collection<JobDetails> allJobs = new LinkedList<>();

            // Fetch all values that store job overviews from RocksDB
            List<String> overviewJsons = kvStore.getAllByPrefix("/jobs/overview/");

            // Iterate over all the overview entries in RocksDB
            for (String overviewJson : overviewJsons) {
                MultipleJobsDetails subJobs =
                        MAPPER.readValue(overviewJson, MultipleJobsDetails.class);
                allJobs.addAll(subJobs.getJobs());
            }

            // Combine all jobs into a single MultipleJobsDetails object
            MultipleJobsDetails combinedOverview = new MultipleJobsDetails(allJobs);

            // Write this combined overview back to RocksDB
            String combinedOverviewJson = MAPPER.writeValueAsString(combinedOverview);
            kvStore.put("/jobs/combined-overview", combinedOverviewJson);

        } catch (Exception e) {
            LOG.error("Failed to update job overview in RocksDB.", e);
        }
    }

    KVStore<String, String> getKVStore() {
        return kvStore;
    }
}
