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
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
 */
public class HistoryServerApplicationArchiveFetcher extends HistoryServerArchiveFetcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(HistoryServerApplicationArchiveFetcher.class);

    private static final String APPLICATIONS_SUBDIR = "applications";
    private static final String APPLICATION_OVERVIEWS_SUBDIR = "application-overviews";

    private final Map<Path, Map<String, Set<String>>> cachedApplicationIdsToJobIds =
            new HashMap<>();

    private final File webApplicationDir;
    private final File webApplicationsOverviewDir;

    HistoryServerApplicationArchiveFetcher(
            List<HistoryServer.RefreshLocation> refreshDirs,
            File webDir,
            Consumer<HistoryServerApplicationArchiveFetcher.ArchiveEvent> archiveEventListener,
            boolean cleanupExpiredArchives,
            ArchiveRetainedStrategy retainedStrategy)
            throws IOException {
        super(refreshDirs, webDir, archiveEventListener, cleanupExpiredArchives, retainedStrategy);

        for (HistoryServer.RefreshLocation refreshDir : refreshDirs) {
            cachedApplicationIdsToJobIds.put(refreshDir.getPath(), new HashMap<>());
        }
        this.webApplicationDir = new File(webDir, APPLICATIONS_SUBDIR);
        Files.createDirectories(webApplicationDir.toPath());
        this.webApplicationsOverviewDir = new File(webDir, APPLICATION_OVERVIEWS_SUBDIR);
        Files.createDirectories(webApplicationsOverviewDir.toPath());
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
            throws IOException {
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
            events.add(processJobArchive(jobId, jobArchive.getPath()));
        }

        return events;
    }

    private ArchiveEvent processApplicationArchive(String applicationId, Path applicationArchive)
            throws IOException {
        for (ArchivedJson archive : FsJsonArchivist.readArchivedJsons(applicationArchive)) {
            String path = archive.getPath();
            String json = archive.getJson();

            File target;
            if (path.equals(ApplicationsOverviewHeaders.URL)) {
                target = new File(webApplicationsOverviewDir, applicationId + JSON_FILE_ENDING);
            } else {
                // this implicitly writes into webApplicationDir
                target = new File(webDir, path + JSON_FILE_ENDING);
            }

            writeTargetFile(target, json);
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
        // Make sure we do not include this application in the overview
        try {
            Files.deleteIfExists(
                    new File(webApplicationsOverviewDir, applicationId + JSON_FILE_ENDING)
                            .toPath());
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from overview directory.", ioe);
        }

        // Clean up application files we may have created
        File applicationDirectory = new File(webApplicationDir, applicationId);
        try {
            FileUtils.deleteDirectory(applicationDirectory);
        } catch (IOException ioe) {
            LOG.warn("Could not clean up application directory.", ioe);
        }

        try {
            Files.deleteIfExists(
                    new File(webApplicationDir, applicationId + JSON_FILE_ENDING).toPath());
        } catch (IOException ioe) {
            LOG.warn("Could not delete file from application directory.", ioe);
        }

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
        try (JsonGenerator gen =
                jacksonFactory.createGenerator(
                        HistoryServer.createOrGetFile(webDir, ApplicationsOverviewHeaders.URL))) {
            File[] overviews = new File(webApplicationsOverviewDir.getPath()).listFiles();
            if (overviews != null) {
                Collection<ApplicationDetails> allApplications = new ArrayList<>(overviews.length);
                for (File overview : overviews) {
                    MultipleApplicationsDetails subApplications =
                            mapper.readValue(overview, MultipleApplicationsDetails.class);
                    allApplications.addAll(subApplications.getApplications());
                }
                mapper.writeValue(gen, new MultipleApplicationsDetails(allApplications));
            }
        } catch (IOException ioe) {
            LOG.error("Failed to update application overview.", ioe);
        }
    }
}
