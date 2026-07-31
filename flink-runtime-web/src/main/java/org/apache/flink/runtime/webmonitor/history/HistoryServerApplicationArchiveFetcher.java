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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.history.ArchivePathUtils;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleApplicationsDetails;
import org.apache.flink.runtime.rest.messages.ApplicationsOverviewHeaders;
import org.apache.flink.runtime.webmonitor.history.retaining.ArchiveRetainedStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.EAGER;
import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.LAZY;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.OVERVIEW_PARSING;

/**
 * This class is used by the {@link HistoryServer} to fetch the application and job archives that
 * are located at {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS}. The directories are
 * polled in regular intervals, defined by {@link
 * HistoryServerOptions#HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL}.
 *
 * <p>The archives are downloaded and expanded into a file structure analog to the REST API.
 *
 * <p>Removes existing archives from these directories and the cache according to {@link
 * ArchiveRetainedStrategy} and {@link
 * HistoryServerOptions#HISTORY_SERVER_CLEANUP_EXPIRED_APPLICATIONS}.
 *
 * @param <Entry> the type of entries returned by the underlying {@link ArchiveStorage}.
 */
public class HistoryServerApplicationArchiveFetcher<Entry>
        extends HistoryServerArchiveFetcher<Entry> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HistoryServerApplicationArchiveFetcher.class);

    protected static final String APPLICATIONS_SUBDIR = "applications";
    protected static final String APPLICATION_OVERVIEWS_SUBDIR = "application-overviews";
    private static final String APPLICATION_KEY_PREFIX = APPLICATIONS_SUBDIR + "/";
    private static final String APPLICATION_OVERVIEWS_KEY_PREFIX =
            APPLICATION_OVERVIEWS_SUBDIR + "/";

    private final Map<Path, Map<String, Set<String>>> cachedApplicationIdsToJobIds =
            new HashMap<>();

    private final ConcurrentHashMap<String, ArchiveMetaInfo> applicationArchiveMetaInfoCache;

    HistoryServerApplicationArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<ArchiveEvent> archiveEventListener,
            boolean cleanupExpiredArchives,
            ArchiveRetainedStrategy retainedStrategy,
            ArchiveStorage<Entry> archiveStorage,
            ConcurrentHashMap<String, ArchiveMetaInfo> archiveMetaInfoCache,
            ConcurrentHashMap<String, ArchiveMetaInfo> applicationArchiveMetaInfoCache,
            int lazyFetchExecutorCommonPoolSize,
            int lazyFetchExecutorIndividualPoolSize)
            throws IOException {
        super(
                refreshDirs,
                webDir,
                archiveEventListener,
                cleanupExpiredArchives,
                retainedStrategy,
                archiveStorage,
                archiveMetaInfoCache,
                lazyFetchExecutorCommonPoolSize,
                lazyFetchExecutorIndividualPoolSize);

        this.applicationArchiveMetaInfoCache = applicationArchiveMetaInfoCache;
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedApplicationIdsToJobIds.put(refreshDir.getPath(), new HashMap<>());
        }
        updateApplicationOverview();
    }

    @Override
    List<FileStatus> listValidArchives(FileSystem refreshFS, Path refreshDir) throws IOException {
        List<FileStatus> applicationArchiveDirs = new ArrayList<>();
        FileStatus[] clusterDirs = refreshFS.listStatus(refreshDir);
        if (clusterDirs == null) {
            // the entire refreshDirectory was removed
            return applicationArchiveDirs;
        }

        // Check for application archive directories in the cluster directories and named according
        // to the application ID format
        for (FileStatus clusterDir : clusterDirs) {
            if (clusterDir.isDir() && isValidId(clusterDir.getPath().getName(), refreshDir)) {
                Path applicationsDir =
                        new Path(clusterDir.getPath(), ArchivePathUtils.APPLICATIONS_DIR);
                FileStatus[] applicationDirs = refreshFS.listStatus(applicationsDir);
                if (applicationDirs == null) {
                    // the entire applicationsDirectory was removed
                    return applicationArchiveDirs;
                }

                for (FileStatus applicationDir : applicationDirs) {
                    if (applicationDir.isDir()
                            && isValidId(applicationDir.getPath().getName(), refreshDir)) {
                        applicationArchiveDirs.add(applicationDir);
                    }
                }
            }
        }

        return applicationArchiveDirs;
    }

    private boolean isValidId(String id, Path refreshDir) {
        try {
            ApplicationID.fromHexString(id);
            return true;
        } catch (IllegalArgumentException iae) {
            LOG.debug(
                    "Archive directory {} contained file with unexpected name {}. Ignoring file.",
                    refreshDir,
                    id,
                    iae);
            return false;
        }
    }

    @Override
    List<ArchiveEvent> processArchive(String archiveId, Path archivePath, Path refreshDir)
            throws Exception {
        return processArchive(archiveId, archivePath, refreshDir, EAGER);
    }

    List<ArchiveEvent> processArchive(
            String archiveId,
            Path archivePath,
            Path refreshDir,
            HistoryServerOptions.HistoryServerArchiveLoadMode archiveLoadMode)
            throws Exception {
        FileSystem fs = archivePath.getFileSystem();
        Path applicationArchive = new Path(archivePath, ArchivePathUtils.APPLICATION_ARCHIVE_NAME);
        if (!fs.exists(applicationArchive)) {
            throw new IOException("Application archive " + applicationArchive + " does not exist.");
        }

        List<ArchiveEvent> events = new ArrayList<>();
        events.add(processApplicationArchive(archiveId, applicationArchive));

        Path jobArchivesDir = new Path(archivePath, ArchivePathUtils.JOBS_DIR);

        List<FileStatus> jobArchives = listValidJobArchives(fs, jobArchivesDir);
        for (FileStatus jobArchive : jobArchives) {
            String jobId = jobArchive.getPath().getName();
            cachedApplicationIdsToJobIds
                    .get(refreshDir)
                    .computeIfAbsent(archiveId, k -> new HashSet<>())
                    .add(jobId);
            ArchiveEvent processArchiveEvents =
                    LAZY.equals(archiveLoadMode)
                            ? lazyProcessJobArchive(jobId, jobArchive.getPath(), false)
                            : processJobArchive(jobId, jobArchive.getPath());
            events.add(processArchiveEvents);
        }

        return events;
    }

    private ArchiveEvent processApplicationArchive(String applicationId, Path applicationArchive)
            throws IOException {
        for (ArchivedJson archive : FsJsonArchivist.readArchivedJsons(applicationArchive)) {
            String path = archive.getPath();
            String json = archive.getJson();

            String key;
            if (path.equals(ApplicationsOverviewHeaders.URL)) {
                key = APPLICATION_OVERVIEWS_KEY_PREFIX + applicationId + JSON_FILE_ENDING;
            } else {
                // the key should be a relative sub-path under the storage root
                key = path.substring(1) + JSON_FILE_ENDING;
            }

            archiveStorage.putArchiveContent(key, json);
        }

        return new ArchiveEvent(applicationId, ArchiveEventType.CREATED);
    }

    @Override
    void deleteFromRemote(Path archive) throws IOException {
        // delete application archive directory recursively (including all its job archives)
        archive.getFileSystem().delete(archive, true);
    }

    @Override
    List<ArchiveEvent> deleteCachedArchives(String archiveId, Path refreshDir) {
        LOG.info("Archive directories for application {} is deleted", archiveId);
        List<ArchiveEvent> deleteLog = new ArrayList<>();

        deleteLog.add(deleteApplicationFiles(archiveId));

        Set<String> jobIds = cachedApplicationIdsToJobIds.get(refreshDir).remove(archiveId);
        if (jobIds != null) {
            jobIds.forEach(jobId -> deleteLog.add(deleteJobFiles(jobId)));
        }

        return deleteLog;
    }

    private ArchiveEvent deleteApplicationFiles(String applicationId) {
        // Delete application overview file in application-overviews directory
        try {
            archiveStorage.delete(
                    APPLICATION_OVERVIEWS_KEY_PREFIX + applicationId + JSON_FILE_ENDING);
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from overview directory.", ioe);
        }

        // Delete application details directory in applications directory,
        // applications/application-id/
        try {
            archiveStorage.deleteEntriesByPrefix(APPLICATION_KEY_PREFIX + applicationId + "/");
        } catch (IOException ioe) {
            LOG.warn("Could not clean up application directory.", ioe);
        }

        // Delete application overview file in applications directory,
        // applications/application-id.json
        try {
            archiveStorage.delete(APPLICATION_KEY_PREFIX + applicationId + JSON_FILE_ENDING);
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from application directory.", ioe);
        }

        applicationArchiveMetaInfoCache.remove(applicationId);
        return new ArchiveEvent(applicationId, ArchiveEventType.DELETED);
    }

    @Override
    void updateOverview() {
        updateApplicationOverview();
        updateJobOverview();
    }

    /**
     * This method replicates the JSON response that would be given by the
     * ApplicationsOverviewHandler when listing applications.
     *
     * <p>Every application archive contains an overview entry with the same structure. Since
     * applications are archived on their own however the list of applications only contains a
     * single application.
     *
     * <p>For the display in the HistoryServer WebFrontend we have to combine these overviews.
     */
    private void updateApplicationOverview() {
        try {
            Collection<ApplicationDetails> allApplications = new ArrayList<>();
            List<Entry> overviews =
                    archiveStorage.getEntriesByPrefix(APPLICATION_OVERVIEWS_KEY_PREFIX);
            for (Entry overview : overviews) {
                MultipleApplicationsDetails subApplications;
                // We treated File as a special case, mainly as a performance trade-off to avoid the
                // overhead of loading the archive into string.
                if (overview instanceof File) {
                    subApplications =
                            mapper.readValue((File) overview, MultipleApplicationsDetails.class);
                } else {
                    subApplications =
                            mapper.readValue(
                                    archiveStorage.readArchiveContent(overview),
                                    MultipleApplicationsDetails.class);
                }
                allApplications.addAll(subApplications.getApplications());
            }
            String overviewWithApplications =
                    mapper.writeValueAsString(new MultipleApplicationsDetails(allApplications));
            archiveStorage.putArchiveContent(
                    ApplicationsOverviewHeaders.URL.substring(1) + JSON_FILE_ENDING,
                    overviewWithApplications);
        } catch (Exception e) {
            LOG.error("Failed to update application overview.", e);
        }
    }

    @Override
    List<ArchiveEvent> lazyProcessArchive(String archiveId, Path archivePath, Path refreshDir)
            throws Exception {
        List<ArchiveEvent> events = new ArrayList<>();
        ArchiveMetaInfo archiveMetaInfo =
                new ArchiveMetaInfo(archiveId, OVERVIEW_PARSING, archivePath);
        ArchiveMetaInfo existing =
                applicationArchiveMetaInfoCache.putIfAbsent(archiveId, archiveMetaInfo);
        if (existing != null) {
            events.add(new ArchiveEvent(archiveId, existing.getEventType()));
            return events;
        }

        events.addAll(processArchive(archiveId, archivePath, refreshDir, LAZY));

        archiveMetaInfo.setEventType(ArchiveEventType.CREATED);
        return events;
    }

    @Override
    void cleanUpLazyFetchTask(String archiveId) {
        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            Path refreshDirPath = refreshDir.getPath();
            if (cachedApplicationIdsToJobIds.get(refreshDirPath).containsKey(archiveId)) {
                Set<String> jobIds =
                        cachedApplicationIdsToJobIds.get(refreshDirPath).get(archiveId);
                jobIds.forEach(super::cleanUpLazyFetchTask);
            }
        }
    }
}
