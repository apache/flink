/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SCAN_CURSOR_BATCH_SIZE;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SCAN_CURSOR_NO_TIMEOUT;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SCAN_FETCH_SIZE;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SCAN_PARTITION_SAMPLES;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SCAN_PARTITION_SIZE;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SCAN_PARTITION_STRATEGY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The configuration class for MongoDB source. */
@PublicEvolving
public class MongoReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fetchSize;

    private final int cursorBatchSize;

    private final boolean noCursorTimeout;

    private final PartitionStrategy partitionStrategy;

    private final MemorySize partitionSize;

    private final int samplesPerPartition;

    private MongoReadOptions(
            int fetchSize,
            int cursorBatchSize,
            boolean noCursorTimeout,
            PartitionStrategy partitionStrategy,
            MemorySize partitionSize,
            int samplesPerPartition) {
        this.fetchSize = fetchSize;
        this.cursorBatchSize = cursorBatchSize;
        this.noCursorTimeout = noCursorTimeout;
        this.partitionStrategy = partitionStrategy;
        this.partitionSize = partitionSize;
        this.samplesPerPartition = samplesPerPartition;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getCursorBatchSize() {
        return cursorBatchSize;
    }

    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    public PartitionStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public MemorySize getPartitionSize() {
        return partitionSize;
    }

    public int getSamplesPerPartition() {
        return samplesPerPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoReadOptions that = (MongoReadOptions) o;
        return cursorBatchSize == that.cursorBatchSize
                && noCursorTimeout == that.noCursorTimeout
                && partitionStrategy == that.partitionStrategy
                && samplesPerPartition == that.samplesPerPartition
                && Objects.equals(partitionSize, that.partitionSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                cursorBatchSize,
                noCursorTimeout,
                partitionStrategy,
                partitionSize,
                samplesPerPartition);
    }

    public static MongoReadOptionsBuilder builder() {
        return new MongoReadOptionsBuilder();
    }

    /** Builder for {@link MongoReadOptions}. */
    public static class MongoReadOptionsBuilder {
        private int fetchSize = SCAN_FETCH_SIZE.defaultValue();
        private int cursorBatchSize = SCAN_CURSOR_BATCH_SIZE.defaultValue();
        private boolean noCursorTimeout = SCAN_CURSOR_NO_TIMEOUT.defaultValue();
        private PartitionStrategy partitionStrategy = SCAN_PARTITION_STRATEGY.defaultValue();
        private MemorySize partitionSize = SCAN_PARTITION_SIZE.defaultValue();
        private int samplesPerPartition = SCAN_PARTITION_SAMPLES.defaultValue();

        /**
         * Sets the number of documents should be fetched per round-trip when reading.
         *
         * @param fetchSize the number of documents should be fetched per round-trip when reading.
         * @return this builder
         */
        public MongoReadOptionsBuilder setFetchSize(int fetchSize) {
            checkArgument(fetchSize > 0, "The fetch size must be larger than 0.");
            this.fetchSize = fetchSize;
            return this;
        }

        /**
         * Sets the batch size of MongoDB find cursor.
         *
         * @param cursorBatchSize the max batch size of find cursor.
         * @return this builder
         */
        public MongoReadOptionsBuilder setCursorBatchSize(int cursorBatchSize) {
            checkArgument(
                    cursorBatchSize >= 0,
                    "The cursor batch size must be larger than or equal to 0.");
            this.cursorBatchSize = cursorBatchSize;
            return this;
        }

        /**
         * Set this option to true to prevent cursor timeout (10 minutes).
         *
         * @param noCursorTimeout Set this option to true to prevent cursor timeout (10 minutes)
         * @return this builder
         */
        public MongoReadOptionsBuilder setNoCursorTimeout(boolean noCursorTimeout) {
            this.noCursorTimeout = noCursorTimeout;
            return this;
        }

        /**
         * Sets the partition strategy.
         *
         * @param partitionStrategy the strategy of a partition.
         * @return this builder
         */
        public MongoReadOptionsBuilder setPartitionStrategy(PartitionStrategy partitionStrategy) {
            checkNotNull(partitionStrategy, "The partition strategy must not be null.");
            this.partitionStrategy = partitionStrategy;
            return this;
        }

        /**
         * Sets the partition size of MongoDB split.
         *
         * @param partitionSize the memory size of a partition.
         * @return this builder
         */
        public MongoReadOptionsBuilder setPartitionSize(MemorySize partitionSize) {
            checkNotNull(partitionSize, "The partition size must not be null");
            checkArgument(
                    partitionSize.getMebiBytes() >= 1,
                    "The partition size must be larger than or equals to 1mb.");
            this.partitionSize = partitionSize;
            return this;
        }

        /**
         * Sets the partition size of MongoDB split.
         *
         * @param samplesPerPartition the memory size of a partition.
         * @return this builder
         */
        public MongoReadOptionsBuilder setSamplesPerPartition(int samplesPerPartition) {
            checkArgument(
                    samplesPerPartition > 0, "The samples per partition must be larger than 0.");
            this.samplesPerPartition = samplesPerPartition;
            return this;
        }

        /**
         * Build the {@link MongoReadOptions}.
         *
         * @return a MongoReadOptions with the settings made for this builder.
         */
        public MongoReadOptions build() {
            return new MongoReadOptions(
                    fetchSize,
                    cursorBatchSize,
                    noCursorTimeout,
                    partitionStrategy,
                    partitionSize,
                    samplesPerPartition);
        }
    }
}
