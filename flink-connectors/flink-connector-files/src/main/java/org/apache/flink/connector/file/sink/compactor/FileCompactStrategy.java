/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.sink.FileSink;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Strategy for compacting the files written in {@link FileSink} before committing. */
@PublicEvolving
public class FileCompactStrategy implements Serializable {

    private static final long serialVersionUID = 1L;

    // Compaction triggering strategies.
    private final long sizeThreshold;
    private final int numCheckpointsBeforeCompaction;

    // Compaction executing strategies.
    private final int numCompactThreads;

    private FileCompactStrategy(
            long sizeThreshold, int numCheckpointsBeforeCompaction, int numCompactThreads) {
        this.sizeThreshold = sizeThreshold;
        this.numCheckpointsBeforeCompaction = numCheckpointsBeforeCompaction;
        this.numCompactThreads = numCompactThreads;
    }

    public long getSizeThreshold() {
        return sizeThreshold;
    }

    public int getNumCheckpointsBeforeCompaction() {
        return numCheckpointsBeforeCompaction;
    }

    public int getNumCompactThreads() {
        return numCompactThreads;
    }

    /** Builder for {@link FileCompactStrategy}. */
    public static class Builder {
        private int numCheckpointsBeforeCompaction = -1;
        private long sizeThreshold = -1;
        private int numCompactThreads = -1;

        public static FileCompactStrategy.Builder newBuilder() {
            return new FileCompactStrategy.Builder();
        }

        /**
         * Optional, compaction will be triggered when N checkpoints passed since the last
         * triggering, -1 by default indicating no compaction on checkpoint.
         */
        public FileCompactStrategy.Builder enableCompactionOnCheckpoint(
                int numCheckpointsBeforeCompaction) {
            checkArgument(
                    numCheckpointsBeforeCompaction > 0,
                    "Number of checkpoints before compaction should be more than 0.");
            this.numCheckpointsBeforeCompaction = numCheckpointsBeforeCompaction;
            return this;
        }

        /**
         * Optional, compaction will be triggered when the total size of compacting files reaches
         * the threshold. -1 by default, indicating the size is unlimited.
         */
        public FileCompactStrategy.Builder setSizeThreshold(long sizeThreshold) {
            this.sizeThreshold = sizeThreshold;
            return this;
        }

        /** Optional, the count of compacting threads in a compactor operator, 1 by default. */
        public FileCompactStrategy.Builder setNumCompactThreads(int numCompactThreads) {
            checkArgument(numCompactThreads > 0, "Compact threads should be more than 0.");
            this.numCompactThreads = numCompactThreads;
            return this;
        }

        public FileCompactStrategy build() {
            validate();
            return new FileCompactStrategy(
                    sizeThreshold, numCheckpointsBeforeCompaction, numCompactThreads);
        }

        private void validate() {
            if (sizeThreshold < 0 && numCheckpointsBeforeCompaction <= 0) {
                throw new IllegalArgumentException(
                        "At least one trigger condition must be configured.");
            }
        }
    }
}
