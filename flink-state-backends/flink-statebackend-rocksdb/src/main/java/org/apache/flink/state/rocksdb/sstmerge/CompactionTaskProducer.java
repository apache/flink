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

import org.apache.flink.shaded.guava32.com.google.common.primitives.UnsignedBytes;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Checks live RocksDB SST files for merging (compacting) according to the {@link
 * RocksDBManualCompactionOptions}. The result is a list of {@link CompactionTask}s.
 *
 * <p>SSTs from all the column families are scanned at once using {@link
 * RocksDB#getLiveFilesMetaData()}. However, each task only contains SSTs of the same level of the
 * same column family.
 *
 * <p>At most {@link RocksDBManualCompactionConfig#maxManualCompactions} tasks are chosen from the
 * candidates with the highest number of files.
 */
class CompactionTaskProducer {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionTaskProducer.class);

    private static final Comparator<SstFileMetaData> SST_COMPARATOR =
            (o1, o2) -> {
                Comparator<byte[]> cmp = UnsignedBytes.lexicographicalComparator();
                int cfCmp = cmp.compare(o1.columnFamilyName(), o2.columnFamilyName());
                if (cfCmp != 0) {
                    return cfCmp;
                } else {
                    return cmp.compare(o1.smallestKey(), o2.smallestKey());
                }
            };

    private final RocksDBManualCompactionConfig settings;
    private final ColumnFamilyLookup columnFamilyLookup;
    private final Supplier<List<SstFileMetaData>> metadataSupplier;

    CompactionTaskProducer(
            RocksDB db,
            RocksDBManualCompactionConfig settings,
            ColumnFamilyLookup columnFamilyLookup) {
        this(
                () -> SstFileMetaData.mapFrom(db.getLiveFilesMetaData()),
                settings,
                columnFamilyLookup);
    }

    CompactionTaskProducer(
            Supplier<List<SstFileMetaData>> metadataSupplier,
            RocksDBManualCompactionConfig settings,
            ColumnFamilyLookup columnFamilyLookup) {
        this.settings = settings;
        this.columnFamilyLookup = columnFamilyLookup;
        this.metadataSupplier = metadataSupplier;
    }

    public List<CompactionTask> produce() {

        // get all CF files sorted by key range start (L1+)
        List<SstFileMetaData> sstSortedByCfAndStartingKeys =
                metadataSupplier.get().stream()
                        .filter(l -> l.level() > 0) // let RocksDB deal with L0
                        .sorted(SST_COMPARATOR)
                        .collect(Collectors.toList());
        LOG.trace("Input files: {}", sstSortedByCfAndStartingKeys.size());

        List<CompactionTask> tasks = groupIntoTasks(sstSortedByCfAndStartingKeys);
        tasks.sort(Comparator.<CompactionTask>comparingInt(t -> t.files.size()).reversed());
        return tasks.subList(0, Math.min(tasks.size(), settings.maxManualCompactions));
    }

    private List<CompactionTask> groupIntoTasks(List<SstFileMetaData> files) {
        // collect the files which won't be compacted by RocksDB
        // and won't cause any problems if merged - i.e. don't overlap with any known files on
        // the other levels
        List<CompactionTask> tasks = new ArrayList<>();
        List<SstFileMetaData> group = new ArrayList<>();
        SstFileMetaData prevFile = null;
        long compactionOutputSize = 0;

        for (SstFileMetaData file : files) {
            final boolean compact = shouldCompact(file);
            final boolean newGroup =
                    !compact || !sameGroup(file, prevFile, group, compactionOutputSize);
            if (newGroup) {
                createTask(group).ifPresent(tasks::add);
                group.clear();
                compactionOutputSize = 0;
            }
            if (compact) {
                group.add(file);
                compactionOutputSize += file.size();
            }
            LOG.trace(
                    "Processed SST file: {}, level={}, cf: {}, being compacted={}, compact: {}, change group: {}, prev level={}",
                    file.fileName(),
                    file.level(),
                    file.columnFamilyName(),
                    file.beingCompacted(),
                    compact,
                    newGroup,
                    prevFile == null ? -1 : prevFile.level());
            prevFile = file;
        }
        createTask(group).ifPresent(tasks::add);
        return tasks;
    }

    private Optional<CompactionTask> createTask(List<SstFileMetaData> compaction) {
        if (compaction.size() < settings.minFilesToCompact) {
            return Optional.empty();
        }
        SstFileMetaData head = compaction.iterator().next();
        ColumnFamilyHandle cf = columnFamilyLookup.get(head.columnFamilyName());
        if (cf == null) {
            LOG.warn("Unknown column family: {}", head.columnFamilyName);
            return Optional.empty();
        }
        List<String> fileNames =
                compaction.stream().map(SstFileMetaData::fileName).collect(Collectors.toList());
        return Optional.of(new CompactionTask(head.level(), fileNames, cf));
    }

    private boolean sameGroup(
            SstFileMetaData file,
            SstFileMetaData prevFile,
            List<SstFileMetaData> group,
            long compactionOutputSize) {
        if (prevFile == null) {
            return true;
        }
        return (file.level() == prevFile.level())
                && Arrays.equals(file.columnFamilyName(), prevFile.columnFamilyName())
                && compactionOutputSize + file.size() <= settings.maxOutputFileSize.getBytes()
                && group.size() < settings.maxFilesToCompact;
    }

    private boolean shouldCompact(SstFileMetaData file) {
        return file.size() <= settings.maxFileSizeToCompact.getBytes() && !file.beingCompacted();
    }

    static class SstFileMetaData {

        private final byte[] columnFamilyName;
        private final String fileName;
        private final int level;
        private final long size;
        private final byte[] smallestKey;
        private final boolean beingCompacted;

        public SstFileMetaData(
                byte[] columnFamilyName,
                String fileName,
                int level,
                long size,
                byte[] smallestKey,
                boolean beingCompacted) {
            this.columnFamilyName = columnFamilyName;
            this.fileName = fileName;
            this.level = level;
            this.size = size;
            this.smallestKey = smallestKey;
            this.beingCompacted = beingCompacted;
        }

        public String fileName() {
            return fileName;
        }

        public byte[] columnFamilyName() {
            return columnFamilyName;
        }

        public int level() {
            return level;
        }

        public long size() {
            return size;
        }

        public byte[] smallestKey() {
            return smallestKey;
        }

        public boolean beingCompacted() {
            return beingCompacted;
        }

        static List<SstFileMetaData> mapFrom(List<LiveFileMetaData> list) {
            return list.stream()
                    .map(SstFileMetaData::fromLiveFileMetaData)
                    .collect(Collectors.toList());
        }

        static SstFileMetaData fromLiveFileMetaData(LiveFileMetaData fileMetaData) {
            return new SstFileMetaData(
                    fileMetaData.columnFamilyName(),
                    fileMetaData.fileName(),
                    fileMetaData.level(),
                    fileMetaData.size(),
                    fileMetaData.smallestKey(),
                    fileMetaData.beingCompacted());
        }
    }
}
