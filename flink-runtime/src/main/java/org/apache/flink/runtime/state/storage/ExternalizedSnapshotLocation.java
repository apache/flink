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

package org.apache.flink.runtime.state.storage;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link ExternalizedSnapshotLocation} configures and validates the base checkpoint- and savepoint
 * directory paths, but also accepts null for both of then, in which case creating externalized
 * checkpoint is not possible, and it is not possible to create a savepoint with a default path.
 */
class ExternalizedSnapshotLocation implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The path where checkpoints will be stored, or null, if none has been configured. */
    @Nullable private Path baseCheckpointPath;

    /** The path where savepoints will be stored, or null, if none has been configured. */
    @Nullable private Path baseSavepointPath;

    @Nullable
    Path getBaseCheckpointPath() {
        return baseCheckpointPath;
    }

    @Nullable
    Path getBaseSavepointPath() {
        return baseSavepointPath;
    }

    private ExternalizedSnapshotLocation() {}

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder {
        @Nullable Path baseCheckpointPath;

        @Nullable Path baseSavepointPath;

        ReadableConfig config = new Configuration();

        Builder withCheckpointPath(@Nullable Path baseCheckpointPath) {
            this.baseCheckpointPath = baseCheckpointPath;
            return this;
        }

        Builder withSavepointPath(@Nullable Path baseSavepointPath) {
            this.baseSavepointPath = baseSavepointPath;
            return this;
        }

        Builder withConfiguration(ReadableConfig config) {
            this.config = config;
            return this;
        }

        ExternalizedSnapshotLocation build() {
            ExternalizedSnapshotLocation location = new ExternalizedSnapshotLocation();
            location.baseCheckpointPath =
                    validatePath(
                            parameterOrConfigured(
                                    baseCheckpointPath,
                                    config,
                                    CheckpointingOptions.CHECKPOINTS_DIRECTORY));
            location.baseSavepointPath =
                    validatePath(
                            parameterOrConfigured(
                                    baseSavepointPath,
                                    config,
                                    CheckpointingOptions.SAVEPOINT_DIRECTORY));
            return location;
        }
    }

    /**
     * Checks the validity of the path's scheme and path.
     *
     * @param path The path to check.
     * @return The URI as a Path.
     * @throws IllegalArgumentException Thrown, if the URI misses scheme or path.
     */
    private static Path validatePath(Path path) {
        if (path == null) {
            return null;
        }

        Optional.ofNullable(path.toUri().getScheme())
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "The scheme (hdfs://, file://, etc) is null. "
                                                + "Please specify the file system scheme explicitly in the URI."));

        Optional.ofNullable(path.getPath())
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "The path to store the checkpoint data in is null. "
                                                + "Please specify a directory path for the checkpoint data."));

        Optional.ofNullable(path.getParent())
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Cannot use the root directory for checkpoints."));

        return path;
    }

    @Nullable
    private static Path parameterOrConfigured(
            @Nullable Path path, ReadableConfig config, ConfigOption<String> option) {

        return Optional.ofNullable(path)
                .orElseGet(
                        () -> {
                            try {
                                return config.getOptional(option).map(Path::new).orElse(null);
                            } catch (IllegalArgumentException e) {
                                throw new IllegalConfigurationException(
                                        "Cannot parse value for "
                                                + option.key()
                                                + " . Not a valid path.",
                                        e);
                            }
                        });
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseCheckpointPath, baseSavepointPath);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ExternalizedSnapshotLocation that = (ExternalizedSnapshotLocation) other;
        return Objects.equals(baseCheckpointPath, that.baseCheckpointPath)
                && Objects.equals(baseSavepointPath, that.baseSavepointPath);
    }
}
