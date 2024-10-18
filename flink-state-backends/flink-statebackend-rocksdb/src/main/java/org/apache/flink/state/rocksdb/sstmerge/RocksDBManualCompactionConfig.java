/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.sstmerge;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

/** Configuration for {@link RocksDBManualCompactionManager}. */
public class RocksDBManualCompactionConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public final long minInterval; // this small value is for testing, prod=10_000 ?
    public final int maxManualCompactions;
    public final MemorySize maxFileSizeToCompact;
    public final int minFilesToCompact; // in a single compaction
    public final int maxFilesToCompact; // in a single compaction
    public final MemorySize maxOutputFileSize;
    public final int maxAutoCompactions;

    public RocksDBManualCompactionConfig(
            long periodMs,
            int maxManualCompactions,
            MemorySize maxFileSizeToCompact,
            int minFilesToCompact,
            int maxFilesToCompact,
            MemorySize maxOutputFileSize,
            int maxAutoCompactions) {
        this.minInterval = periodMs;
        this.maxManualCompactions = maxManualCompactions;
        this.maxFileSizeToCompact = maxFileSizeToCompact;
        this.minFilesToCompact = minFilesToCompact;
        this.maxFilesToCompact = maxFilesToCompact;
        this.maxOutputFileSize = maxOutputFileSize;
        this.maxAutoCompactions = maxAutoCompactions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RocksDBManualCompactionConfig from(ReadableConfig config) {
        return builder()
                .setMinInterval(config.get(RocksDBManualCompactionOptions.MIN_INTERVAL).toMillis())
                .setMaxParallelCompactions(
                        config.get(RocksDBManualCompactionOptions.MAX_PARALLEL_COMPACTIONS))
                .setMaxFileSizeToCompact(
                        config.get(RocksDBManualCompactionOptions.MAX_FILE_SIZE_TO_COMPACT))
                .setMaxFilesToCompact(
                        config.get(RocksDBManualCompactionOptions.MAX_FILES_TO_COMPACT))
                .setMinFilesToCompact(
                        config.get(RocksDBManualCompactionOptions.MIN_FILES_TO_COMPACT))
                .setMaxOutputFileSize(
                        config.get(RocksDBManualCompactionOptions.MAX_OUTPUT_FILE_SIZE))
                .setMaxAutoCompactions(
                        config.get(RocksDBManualCompactionOptions.MAX_AUTO_COMPACTIONS))
                .build();
    }

    public static RocksDBManualCompactionConfig getDefault() {
        return builder().build();
    }

    @Override
    public String toString() {
        return "RocksDBManualCompactionConfig{"
                + "minInterval="
                + minInterval
                + ", maxManualCompactions="
                + maxManualCompactions
                + ", maxFileSizeToCompact="
                + maxFileSizeToCompact
                + ", minFilesToCompact="
                + minFilesToCompact
                + ", maxFilesToCompact="
                + maxFilesToCompact
                + ", maxOutputFileSize="
                + maxOutputFileSize
                + ", maxAutoCompactions="
                + maxAutoCompactions
                + '}';
    }

    /**
     * Builder for {@link org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionConfig}.
     */
    public static class Builder {
        private long minInterval =
                RocksDBManualCompactionOptions.MIN_INTERVAL.defaultValue().toMillis();
        private int maxParallelCompactions =
                RocksDBManualCompactionOptions.MAX_PARALLEL_COMPACTIONS.defaultValue();
        private MemorySize maxFileSizeToCompact =
                RocksDBManualCompactionOptions.MAX_FILE_SIZE_TO_COMPACT.defaultValue();
        private int minFilesToCompact =
                RocksDBManualCompactionOptions.MIN_FILES_TO_COMPACT.defaultValue();
        private int maxFilesToCompact =
                RocksDBManualCompactionOptions.MAX_FILES_TO_COMPACT.defaultValue();
        private MemorySize maxOutputFileSize =
                RocksDBManualCompactionOptions.MAX_OUTPUT_FILE_SIZE.defaultValue();
        private int maxAutoCompactions =
                RocksDBManualCompactionOptions.MAX_AUTO_COMPACTIONS.defaultValue();

        public Builder setMinInterval(long minInterval) {
            this.minInterval = minInterval;
            return this;
        }

        public Builder setMaxParallelCompactions(int maxParallelCompactions) {
            this.maxParallelCompactions = maxParallelCompactions;
            return this;
        }

        public Builder setMaxFileSizeToCompact(MemorySize maxFileSizeToCompact) {
            this.maxFileSizeToCompact = maxFileSizeToCompact;
            return this;
        }

        public Builder setMinFilesToCompact(int minFilesToCompact) {
            this.minFilesToCompact = minFilesToCompact;
            return this;
        }

        public Builder setMaxFilesToCompact(int maxFilesToCompact) {
            this.maxFilesToCompact = maxFilesToCompact;
            return this;
        }

        public Builder setMaxOutputFileSize(MemorySize maxOutputFileSize) {
            this.maxOutputFileSize = maxOutputFileSize;
            return this;
        }

        public Builder setMaxAutoCompactions(int maxAutoCompactions) {
            this.maxAutoCompactions = maxAutoCompactions;
            return this;
        }

        public RocksDBManualCompactionConfig build() {
            return new RocksDBManualCompactionConfig(
                    minInterval,
                    maxParallelCompactions,
                    maxFileSizeToCompact,
                    minFilesToCompact,
                    maxFilesToCompact,
                    maxOutputFileSize,
                    maxAutoCompactions);
        }
    }
}
