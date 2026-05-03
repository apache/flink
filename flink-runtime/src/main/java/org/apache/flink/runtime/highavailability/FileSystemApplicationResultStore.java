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
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.rest.messages.json.ApplicationResultDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationResultSerializer;
import org.apache.flink.runtime.util.NonClosingOutputStreamDecorator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * An implementation of the {@link ApplicationResultStore} which persists application result data to
 * an underlying distributed filesystem.
 */
public class FileSystemApplicationResultStore extends AbstractThreadsafeApplicationResultStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(FileSystemApplicationResultStore.class);

    @VisibleForTesting static final String FILE_EXTENSION = ".json";
    @VisibleForTesting static final String DIRTY_FILE_EXTENSION = "_DIRTY" + FILE_EXTENSION;

    @VisibleForTesting
    public static boolean hasValidDirtyApplicationResultStoreEntryExtension(String filename) {
        return filename.endsWith(DIRTY_FILE_EXTENSION);
    }

    @VisibleForTesting
    public static boolean hasValidApplicationResultStoreEntryExtension(String filename) {
        return filename.endsWith(FILE_EXTENSION);
    }

    private final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    private final FileSystem fileSystem;

    private volatile boolean basePathCreated;

    private final Path basePath;

    private final boolean deleteOnCommit;

    @VisibleForTesting
    FileSystemApplicationResultStore(
            FileSystem fileSystem, Path basePath, boolean deleteOnCommit, Executor ioExecutor) {
        super(ioExecutor);
        this.fileSystem = fileSystem;
        this.basePath = basePath;
        this.deleteOnCommit = deleteOnCommit;
    }

    public static FileSystemApplicationResultStore fromConfiguration(
            Configuration config, Executor ioExecutor) throws IOException {
        Preconditions.checkNotNull(config);
        final String arsStoragePath = config.get(ApplicationResultStoreOptions.STORAGE_PATH);
        final Path basePath;

        if (isNullOrWhitespaceOnly(arsStoragePath)) {
            final String haStoragePath = config.get(HighAvailabilityOptions.HA_STORAGE_PATH);
            final String haClusterId = config.get(HighAvailabilityOptions.HA_CLUSTER_ID);
            basePath =
                    new Path(createDefaultApplicationResultStorePath(haStoragePath, haClusterId));
        } else {
            basePath = new Path(arsStoragePath);
        }

        boolean deleteOnCommit = config.get(ApplicationResultStoreOptions.DELETE_ON_COMMIT);

        return new FileSystemApplicationResultStore(
                basePath.getFileSystem(), basePath, deleteOnCommit, ioExecutor);
    }

    private void createBasePathIfNeeded() throws IOException {
        if (!basePathCreated) {
            LOG.info(
                    "Creating highly available application result storage directory at {}",
                    basePath);
            fileSystem.mkdirs(basePath);
            LOG.info(
                    "Created highly available application result storage directory at {}",
                    basePath);
            basePathCreated = true;
        }
    }

    public static String createDefaultApplicationResultStorePath(String baseDir, String clusterId) {
        return baseDir + "/application-result-store/" + clusterId;
    }

    /**
     * Given an application ID, construct the path for a dirty entry corresponding to it in the
     * application result store.
     *
     * @param applicationId The application ID to construct a dirty entry path from.
     * @return A path for a dirty entry for the given the Application ID.
     */
    private Path constructDirtyPath(ApplicationID applicationId) {
        return constructEntryPath(applicationId.toString() + DIRTY_FILE_EXTENSION);
    }

    /**
     * Given an application ID, construct the path for a clean entry corresponding to it in the
     * application result store.
     *
     * @param applicationId The application ID to construct a clean entry path from.
     * @return A path for a clean entry for the given the Application ID.
     */
    private Path constructCleanPath(ApplicationID applicationId) {
        return constructEntryPath(applicationId.toString() + FILE_EXTENSION);
    }

    @VisibleForTesting
    Path constructEntryPath(String fileName) {
        return new Path(this.basePath, fileName);
    }

    @Override
    public void createDirtyResultInternal(ApplicationResultEntry applicationResultEntry)
            throws IOException {
        createBasePathIfNeeded();

        final Path path = constructDirtyPath(applicationResultEntry.getApplicationId());
        try (OutputStream os = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            mapper.writeValue(
                    // working around the internally used _writeAndClose method to ensure that close
                    // is only called once
                    new NonClosingOutputStreamDecorator(os),
                    new JsonApplicationResultEntry(applicationResultEntry.getApplicationResult()));
        }
    }

    @Override
    public void markResultAsCleanInternal(ApplicationID applicationId)
            throws IOException, NoSuchElementException {
        Path dirtyPath = constructDirtyPath(applicationId);

        if (!fileSystem.exists(dirtyPath)) {
            throw new NoSuchElementException(
                    String.format(
                            "Could not mark application %s as clean as it is not present in the application result store.",
                            applicationId));
        }

        if (deleteOnCommit) {
            fileSystem.delete(dirtyPath, false);
        } else {
            fileSystem.rename(dirtyPath, constructCleanPath(applicationId));
        }
    }

    @Override
    public boolean hasDirtyApplicationResultEntryInternal(ApplicationID applicationId)
            throws IOException {
        return fileSystem.exists(constructDirtyPath(applicationId));
    }

    @Override
    public boolean hasCleanApplicationResultEntryInternal(ApplicationID applicationId)
            throws IOException {
        return fileSystem.exists(constructCleanPath(applicationId));
    }

    @Override
    public Set<ApplicationResult> getDirtyResultsInternal() throws IOException {
        createBasePathIfNeeded();

        final FileStatus[] statuses = fileSystem.listStatus(this.basePath);

        Preconditions.checkState(
                statuses != null,
                "The base directory of the ApplicationResultStore isn't accessible. No dirty ApplicationResults can be restored.");

        final Set<ApplicationResult> dirtyResults = new HashSet<>();
        for (FileStatus s : statuses) {
            if (!s.isDir()) {
                if (hasValidDirtyApplicationResultStoreEntryExtension(s.getPath().getName())) {
                    JsonApplicationResultEntry jre =
                            mapper.readValue(
                                    fileSystem.open(s.getPath()), JsonApplicationResultEntry.class);
                    ApplicationResult applicationResult = jre.getApplicationResult();
                    if (applicationResult != null) {
                        dirtyResults.add(applicationResult);
                    }
                }
            }
        }
        return dirtyResults;
    }

    @Override
    protected ApplicationResult getCleanApplicationResultInternal(ApplicationID applicationId)
            throws IOException {
        if (deleteOnCommit) {
            return null;
        }

        Path cleanPath = constructCleanPath(applicationId);
        if (!fileSystem.exists(cleanPath)) {
            return null;
        }

        JsonApplicationResultEntry jre =
                mapper.readValue(fileSystem.open(cleanPath), JsonApplicationResultEntry.class);
        return jre.getApplicationResult();
    }

    /**
     * Wrapper class around {@link ApplicationResultEntry} to allow for serialization of a schema
     * version, so that future schema changes can be handled in a backwards compatible manner.
     */
    @JsonIgnoreProperties(
            value = {JsonApplicationResultEntry.FIELD_NAME_VERSION},
            allowGetters = true)
    @VisibleForTesting
    static class JsonApplicationResultEntry extends ApplicationResultEntry {
        private static final String FIELD_NAME_RESULT = "result";
        static final String FIELD_NAME_VERSION = "version";

        @JsonCreator
        private JsonApplicationResultEntry(
                @JsonProperty(FIELD_NAME_RESULT) ApplicationResult applicationResult) {
            super(applicationResult);
        }

        @Override
        @JsonProperty(FIELD_NAME_RESULT)
        @JsonSerialize(using = ApplicationResultSerializer.class)
        @JsonDeserialize(using = ApplicationResultDeserializer.class)
        public ApplicationResult getApplicationResult() {
            return super.getApplicationResult();
        }

        @JsonIgnore
        @Override
        public ApplicationID getApplicationId() {
            return super.getApplicationId();
        }

        public int getVersion() {
            return 1;
        }
    }
}
