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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Compacts multiple RocksDB SST files using {@link RocksDB#compactFiles(CompactionOptions,
 * ColumnFamilyHandle, List, int, int, CompactionJobInfo) RocksDB#compactFiles} into the last level.
 * Usually this results in a single SST file if it doesn't exceed RocksDB target output file size
 * for that level.
 */
class Compactor {
    private static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
    private static final int OUTPUT_PATH_ID = 0; // just use the first one

    private final CompactionTarget db;
    private final long targetOutputFileSize;

    public Compactor(RocksDB db, long targetOutputFileSize) {
        this(db::compactFiles, targetOutputFileSize);
    }

    public Compactor(CompactionTarget target, long targetOutputFileSize) {
        this.db = target;
        this.targetOutputFileSize = targetOutputFileSize;
    }

    void compact(ColumnFamilyHandle cfName, int level, List<String> files) throws RocksDBException {
        int outputLevel = Math.min(level + 1, cfName.getDescriptor().getOptions().numLevels() - 1);
        LOG.debug(
                "Manually compacting {} files from level {} to {}: {}",
                files.size(),
                level,
                outputLevel,
                files);
        try (CompactionOptions options =
                        new CompactionOptions().setOutputFileSizeLimit(targetOutputFileSize);
                CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
            db.compactFiles(options, cfName, files, outputLevel, OUTPUT_PATH_ID, compactionJobInfo);
        }
    }

    public interface CompactionTarget {
        void compactFiles(
                CompactionOptions var1,
                ColumnFamilyHandle var2,
                List<String> var3,
                int var4,
                int var5,
                CompactionJobInfo var6)
                throws RocksDBException;

        CompactionTarget NO_OP = (var1, var2, var3, var4, var5, var6) -> {};
    }
}
