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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.json.JobResultDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobResultSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * An implementation of the {@link JobResultStore} which persists job result data to an underlying
 * distributed filesystem.
 */
public class FileSystemJobResultStore extends AbstractThreadsafeJobResultStore {

    @VisibleForTesting static final String FILE_EXTENSION = ".json";
    @VisibleForTesting static final String DIRTY_FILE_EXTENSION = "_DIRTY" + FILE_EXTENSION;

    @VisibleForTesting
    public static boolean hasValidDirtyJobResultStoreEntryExtension(String filename) {
        return filename.endsWith(DIRTY_FILE_EXTENSION);
    }

    @VisibleForTesting
    public static boolean hasValidJobResultStoreEntryExtension(String filename) {
        return filename.endsWith(FILE_EXTENSION);
    }

    private final ObjectMapper mapper = new ObjectMapper();

    private final FileSystem fileSystem;

    private final Path basePath;

    private final boolean deleteOnCommit;

    @VisibleForTesting
    FileSystemJobResultStore(FileSystem fileSystem, Path basePath, boolean deleteOnCommit)
            throws IOException {
        this.fileSystem = fileSystem;
        this.basePath = basePath;
        this.deleteOnCommit = deleteOnCommit;

        this.fileSystem.mkdirs(this.basePath);
    }

    public static FileSystemJobResultStore fromConfiguration(Configuration config)
            throws IOException {
        Preconditions.checkNotNull(config);
        final String jrsStoragePath = config.get(JobResultStoreOptions.STORAGE_PATH);
        final Path basePath;

        if (isNullOrWhitespaceOnly(jrsStoragePath)) {
            final String haStoragePath = config.get(HighAvailabilityOptions.HA_STORAGE_PATH);
            final String haClusterId = config.get(HighAvailabilityOptions.HA_CLUSTER_ID);
            basePath = new Path(createDefaultJobResultStorePath(haStoragePath, haClusterId));
        } else {
            basePath = new Path(jrsStoragePath);
        }

        boolean deleteOnCommit = config.get(JobResultStoreOptions.DELETE_ON_COMMIT);

        return new FileSystemJobResultStore(basePath.getFileSystem(), basePath, deleteOnCommit);
    }

    public static String createDefaultJobResultStorePath(String baseDir, String clusterId) {
        return baseDir + "/job-result-store/" + clusterId;
    }

    /**
     * Given a job ID, construct the path for a dirty entry corresponding to it in the job result
     * store.
     *
     * @param jobId The job ID to construct a dirty entry path from.
     * @return A path for a dirty entry for the given the Job ID.
     */
    private Path constructDirtyPath(JobID jobId) {
        return new Path(this.basePath.getPath(), jobId.toString() + DIRTY_FILE_EXTENSION);
    }

    /**
     * Given a job ID, construct the path for a clean entry corresponding to it in the job result
     * store.
     *
     * @param jobId The job ID to construct a clean entry path from.
     * @return A path for a clean entry for the given the Job ID.
     */
    private Path constructCleanPath(JobID jobId) {
        return new Path(this.basePath.getPath(), jobId.toString() + ".json");
    }

    @Override
    public void createDirtyResultInternal(JobResultEntry jobResultEntry) throws IOException {
        final Path path = constructDirtyPath(jobResultEntry.getJobId());
        try (OutputStream os = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            mapper.writeValue(os, new JsonJobResultEntry(jobResultEntry));
            os.flush();
        }
    }

    @Override
    public void markResultAsCleanInternal(JobID jobId) throws IOException, NoSuchElementException {
        Path dirtyPath = constructDirtyPath(jobId);

        if (!fileSystem.exists(dirtyPath)) {
            throw new NoSuchElementException(
                    String.format(
                            "Could not mark job %s as clean as it is not present in the job result store.",
                            jobId));
        }

        if (deleteOnCommit) {
            fileSystem.delete(dirtyPath, false);
        } else {
            fileSystem.rename(dirtyPath, constructCleanPath(jobId));
        }
    }

    @Override
    public boolean hasDirtyJobResultEntryInternal(JobID jobId) throws IOException {
        return fileSystem.exists(constructDirtyPath(jobId));
    }

    @Override
    public boolean hasCleanJobResultEntryInternal(JobID jobId) throws IOException {
        return fileSystem.exists(constructCleanPath(jobId));
    }

    @Override
    public Set<JobResult> getDirtyResultsInternal() throws IOException {
        final FileStatus[] statuses = fileSystem.listStatus(this.basePath);

        Preconditions.checkState(
                statuses != null,
                "The base directory of the JobResultStore isn't accessible. No dirty JobResults can be restored.");

        final Set<JobResult> dirtyResults = new HashSet<>();
        for (FileStatus s : statuses) {
            if (!s.isDir()) {
                if (hasValidDirtyJobResultStoreEntryExtension(s.getPath().getName())) {
                    JsonJobResultEntry jre =
                            mapper.readValue(
                                    fileSystem.open(s.getPath()), JsonJobResultEntry.class);
                    dirtyResults.add(jre.getJobResult());
                }
            }
        }
        return dirtyResults;
    }

    /**
     * Wrapper class around {@link JobResultEntry} to allow for serialization of a schema version,
     * so that future schema changes can be handled in a backwards compatible manner.
     */
    @JsonIgnoreProperties(
            value = {JsonJobResultEntry.FIELD_NAME_VERSION},
            allowGetters = true)
    @VisibleForTesting
    static class JsonJobResultEntry extends JobResultEntry {
        private static final String FIELD_NAME_RESULT = "result";
        private static final String FIELD_NAME_VERSION = "version";

        private JsonJobResultEntry(JobResultEntry entry) {
            this(entry.getJobResult());
        }

        @JsonCreator
        private JsonJobResultEntry(@JsonProperty(FIELD_NAME_RESULT) JobResult jobResult) {
            super(jobResult);
        }

        @Override
        @JsonProperty(FIELD_NAME_RESULT)
        @JsonSerialize(using = JobResultSerializer.class)
        @JsonDeserialize(using = JobResultDeserializer.class)
        public JobResult getJobResult() {
            return super.getJobResult();
        }

        @JsonIgnore
        @Override
        public JobID getJobId() {
            return super.getJobId();
        }

        public int getVersion() {
            return 1;
        }
    }
}
