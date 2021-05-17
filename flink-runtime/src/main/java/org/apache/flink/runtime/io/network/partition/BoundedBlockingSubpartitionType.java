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

package org.apache.flink.runtime.io.network.partition;

import java.io.File;
import java.io.IOException;

/** The type of the BoundedBlockingSubpartition. Also doubles as the factory. */
public enum BoundedBlockingSubpartitionType {

    /**
     * A BoundedBlockingSubpartition type that simply stores the partition data in a file. Data is
     * eagerly spilled (written to disk) and readers directly read from the file.
     */
    FILE {

        @Override
        public BoundedBlockingSubpartition create(
                int index,
                ResultPartition parent,
                File tempFile,
                int readBufferSize,
                boolean sslEnabled)
                throws IOException {

            return BoundedBlockingSubpartition.createWithFileChannel(
                    index, parent, tempFile, readBufferSize, sslEnabled);
        }
    },

    /**
     * A BoundedBlockingSubpartition type that stores the partition data in memory mapped file. Data
     * is written to and read from the mapped memory region. Disk spilling happens lazily, when the
     * OS swaps out the pages from the memory mapped file.
     */
    MMAP {

        @Override
        public BoundedBlockingSubpartition create(
                int index,
                ResultPartition parent,
                File tempFile,
                int readBufferSize,
                boolean sslEnabled)
                throws IOException {

            return BoundedBlockingSubpartition.createWithMemoryMappedFile(index, parent, tempFile);
        }
    },

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in a file and memory
     * maps that file for reading. Data is eagerly spilled (written to disk) and then mapped into
     * memory. The main difference to the {@link BoundedBlockingSubpartitionType#MMAP} variant is
     * that no I/O is necessary when pages from the memory mapped file are evicted.
     */
    FILE_MMAP {

        @Override
        public BoundedBlockingSubpartition create(
                int index,
                ResultPartition parent,
                File tempFile,
                int readBufferSize,
                boolean sslEnabled)
                throws IOException {

            return BoundedBlockingSubpartition.createWithFileAndMemoryMappedReader(
                    index, parent, tempFile);
        }
    },

    /**
     * Selects the BoundedBlockingSubpartition type based on the current memory architecture. If
     * 64-bit, the type of {@link BoundedBlockingSubpartitionType#FILE_MMAP} is recommended.
     * Otherwise, the type of {@link BoundedBlockingSubpartitionType#FILE} is by default.
     */
    AUTO {

        @Override
        public BoundedBlockingSubpartition create(
                int index,
                ResultPartition parent,
                File tempFile,
                int readBufferSize,
                boolean sslEnabled)
                throws IOException {

            return ResultPartitionFactory.getBoundedBlockingType()
                    .create(index, parent, tempFile, readBufferSize, sslEnabled);
        }
    };

    // ------------------------------------------------------------------------

    /** Creates BoundedBlockingSubpartition of this type. */
    public abstract BoundedBlockingSubpartition create(
            int index,
            ResultPartition parent,
            File tempFile,
            int readBufferSize,
            boolean sslEnabled)
            throws IOException;
}
