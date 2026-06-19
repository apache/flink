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
import org.apache.flink.state.rocksdb.RocksDBExtension;
import org.apache.flink.state.rocksdb.sstmerge.CompactionTaskProducer.SstFileMetaData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link CompactionTaskProducer} test. */
class CompactionTaskProducerTest {
    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();
    private static final Random RANDOM = new Random();

    private ColumnFamilyLookup defaultCfLookup;

    @BeforeEach
    public void beforeEach() {
        defaultCfLookup = new ColumnFamilyLookup();
        defaultCfLookup.add(rocksDBExtension.getDefaultColumnFamily());
    }

    @Test
    void testEmpty() {
        assertThat(produce(configBuilder().build())).isEmpty();
    }

    @Test
    void testSingleFile() {
        assertThat(produce(configBuilder().build(), sstBuilder().build())).isNotEmpty();
    }

    @Test
    void testMinFilesToCompact() {
        assertThat(produce(configBuilder().setMinFilesToCompact(2).build(), sstBuilder().build()))
                .isEmpty();
    }

    @Test
    void testMaxFilesToCompact() {
        assertThat(
                        produce(
                                configBuilder().setMaxFilesToCompact(1).build(),
                                sstBuilder().build(),
                                sstBuilder().build()))
                .hasSize(2);
    }

    @Test
    void testMaxParallelCompactions() {
        assertThat(
                        produce(
                                configBuilder()
                                        .setMaxFilesToCompact(1)
                                        .setMaxParallelCompactions(2)
                                        .build(),
                                sstBuilder().build(),
                                sstBuilder().build(),
                                sstBuilder().build(),
                                sstBuilder().build()))
                .hasSize(2);
    }

    @Test
    void testMaxFileSizeToCompact() {
        assertThat(
                        produce(
                                configBuilder().setMaxFileSizeToCompact(new MemorySize(1)).build(),
                                sstBuilder().build()))
                .isEmpty();
    }

    @Test
    void testMaxOutputFileSize() {
        final int numFiles = 5;
        final int fileSize = 10;
        final long totalSize = fileSize * numFiles;
        assertThat(
                        produce(
                                configBuilder()
                                        .setMaxOutputFileSize(new MemorySize(totalSize - 1))
                                        .build(),
                                buildSstFiles(1, fileSize, numFiles)))
                .hasSize(2);
    }

    @Test
    void testGrouping() {
        int level = 3;
        SstFileMetaData[] files = buildSstFiles(level, 1024, 10);
        assertThat(produce(configBuilder().build(), files))
                .hasSameElementsAs(singletonList(createTask(level, files)));
    }

    @Test
    void testGroupingWithGap() {
        SstFileMetaData sst0 = sstBuilder().setLevel(1).setSmallestKey("0".getBytes()).build();
        SstFileMetaData sst1 = sstBuilder().setLevel(1).setSmallestKey("1".getBytes()).build();
        SstFileMetaData sst2 =
                sstBuilder()
                        .setLevel(2)
                        .setSmallestKey("2".getBytes())
                        .setBeingCompacted(true)
                        .build();
        SstFileMetaData sst3 = sstBuilder().setLevel(3).setSmallestKey("3".getBytes()).build();
        assertThat(produce(configBuilder().build(), sst0, sst1, sst2, sst3))
                .hasSameElementsAs(
                        Arrays.asList(
                                createTask(sst0.level(), sst0, sst1),
                                createTask(sst3.level(), sst3)));
    }

    @Test
    void testNotGroupingOnDifferentLevels() {
        SstFileMetaData sst1 = sstBuilder().setLevel(1).build();
        SstFileMetaData sst2 = sstBuilder().setLevel(2).build();
        assertThat(produce(configBuilder().build(), sst1, sst2)).hasSize(2);
    }

    @Test
    void testSkipBeingCompacted() {
        assertThat(produce(configBuilder().build(), sstBuilder().setBeingCompacted(true).build()))
                .isEmpty();
    }

    @Test
    void testSkipZeroLevel() {
        assertThat(produce(configBuilder().build(), sstBuilder().setLevel(0).build())).isEmpty();
    }

    @Test
    void testNotGroupingDifferentColumnFamilies() {
        ColumnFamilyHandle cf1 = rocksDBExtension.createNewColumnFamily("cf1");
        defaultCfLookup.add(cf1);
        ColumnFamilyHandle cf2 = rocksDBExtension.createNewColumnFamily("cf2");
        defaultCfLookup.add(cf2);
        assertThat(
                        produce(
                                configBuilder().build(),
                                sstBuilder().setColumnFamily(cf1).build(),
                                sstBuilder().setColumnFamily(cf2).build()))
                .hasSize(2);
    }

    //////////////////
    // utility methods

    static class SstFileMetaDataBuilder {
        private byte[] columnFamilyName;
        private String fileName;
        private int level;
        private long size;
        private byte[] smallestKey;
        private boolean beingCompacted;

        public SstFileMetaDataBuilder(ColumnFamilyHandle columnFamily) {
            try {
                this.columnFamilyName = columnFamily.getName();
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        public SstFileMetaDataBuilder setColumnFamily(ColumnFamilyHandle columnFamily) {
            try {
                this.columnFamilyName = columnFamily.getName();
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public SstFileMetaDataBuilder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public SstFileMetaDataBuilder setLevel(int level) {
            this.level = level;
            return this;
        }

        public SstFileMetaDataBuilder setSize(long size) {
            this.size = size;
            return this;
        }

        public SstFileMetaDataBuilder setSmallestKey(byte[] smallestKey) {
            this.smallestKey = smallestKey;
            return this;
        }

        public SstFileMetaDataBuilder setBeingCompacted(boolean beingCompacted) {
            this.beingCompacted = beingCompacted;
            return this;
        }

        public SstFileMetaData build() {
            return new SstFileMetaData(
                    columnFamilyName, fileName, level, size, smallestKey, beingCompacted);
        }
    }

    private List<CompactionTask> produce(
            RocksDBManualCompactionConfig config, SstFileMetaData... sst) {
        return new CompactionTaskProducer(() -> Arrays.asList(sst), config, defaultCfLookup)
                .produce();
    }

    private static RocksDBManualCompactionConfig.Builder configBuilder() {
        return RocksDBManualCompactionConfig.builder()
                .setMaxFilesToCompact(Integer.MAX_VALUE)
                .setMaxAutoCompactions(Integer.MAX_VALUE)
                .setMaxParallelCompactions(Integer.MAX_VALUE)
                .setMaxOutputFileSize(MemorySize.MAX_VALUE)
                .setMinFilesToCompact(1)
                .setMinInterval(1L);
    }

    private SstFileMetaDataBuilder sstBuilder() {
        byte[] bytes = new byte[128];
        RANDOM.nextBytes(bytes);
        return new SstFileMetaDataBuilder(rocksDBExtension.getDefaultColumnFamily())
                .setFileName(RANDOM.nextInt() + ".sst")
                .setLevel(1)
                .setSize(4)
                .setSmallestKey(bytes);
    }

    private SstFileMetaData[] buildSstFiles(int level, int fileSize, int numFiles) {
        return IntStream.range(0, numFiles)
                .mapToObj(
                        i ->
                                sstBuilder()
                                        .setSmallestKey(new byte[] {(byte) i})
                                        .setLevel(level)
                                        .setSize(fileSize)
                                        .build())
                .toArray(SstFileMetaData[]::new);
    }

    private CompactionTask createTask(int level, SstFileMetaData... files) {
        return new CompactionTask(
                level,
                Arrays.stream(files).map(SstFileMetaData::fileName).collect(Collectors.toList()),
                rocksDBExtension.getDefaultColumnFamily());
    }
}
