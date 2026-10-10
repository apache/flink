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

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.enumerate.GlobFileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests FileSource glob discovery and complete block coverage against a real HDFS namespace. */
@Timeout(120)
class FileSourceHdfsGlobITCase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @TempDir private java.nio.file.Path temporaryFolder;

    private MiniDFSCluster cluster;

    @BeforeEach
    void startHdfs() throws Exception {
        assumeThat(OperatingSystem.isWindows())
                .as("HDFS requires native extensions on Windows.")
                .isFalse();
        final Configuration configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, temporaryFolder.toString());
        configuration.setLong("dfs.namenode.fs-limits.min-block-size", 1024);
        cluster = new MiniDFSCluster.Builder(configuration).numDataNodes(1).build();
        cluster.waitActive();
    }

    @AfterEach
    void stopHdfs() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    void testBoundedReadingAndAllBlocks() throws Exception {
        final String largeFileContents = "first\n".repeat(1000);
        write("/glob/part-1/data.txt", largeFileContents);
        write("/glob/part-2/data.txt", "second\n");
        write("/glob/archive/part-3/data.txt", "unselected\n");
        write("/glob/part-1/_hidden.txt", "hidden\n");

        final Path pattern = path("/glob/part-*/*.txt");
        assertThat(pattern.getFileSystem()).isInstanceOf(HadoopFileSystem.class);

        final Collection<FileSourceSplit> splits =
                new GlobFileEnumerator(new BlockSplittingRecursiveEnumerator())
                        .enumerateSplits(new Path[] {pattern}, 4);
        final List<FileSourceSplit> largeFileSplits = new ArrayList<>();
        for (FileSourceSplit split : splits) {
            if (split.path().equals(path("/glob/part-1/data.txt"))) {
                largeFileSplits.add(split);
            }
        }
        largeFileSplits.sort(Comparator.comparingLong(FileSourceSplit::offset));
        assertThat(largeFileSplits).hasSizeGreaterThan(1);
        long offset = 0;
        for (FileSourceSplit split : largeFileSplits) {
            assertThat(split.offset()).isEqualTo(offset);
            offset += split.length();
        }
        assertThat(offset).isEqualTo(largeFileContents.getBytes(StandardCharsets.UTF_8).length);
        assertThat(splits).extracting(FileSourceSplit::splitId).doesNotHaveDuplicates();

        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            expected.add("first");
        }
        expected.add("second");
        assertThat(read(pattern)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testLiteralCharactersDirectoryMatchesAndNoMatches() throws Exception {
        write("/glob/{literal}/report[2026].txt", "literal\n");
        write("/glob/{literal}/report2.txt", "other\n");
        write("/glob/part-1/nested/data.txt", "nested\n");

        assertThat(read(path("/glob/{literal}/report[[]2026].txt"))).containsExactly("literal");
        assertThat(read(path("/glob/part-*"))).containsExactly("nested");
        assertThat(read(path("/missing/part-*/*.txt"))).isEmpty();

        assertThat(
                        new NonSplittingRecursiveEnumerator()
                                .enumerateSplits(
                                        new Path[] {path("/glob/{literal}/report[2026].txt")}, 1))
                .extracting(FileSourceSplit::path)
                .containsExactly(path("/glob/{literal}/report[2026].txt"));
    }

    private List<String> read(Path pattern) throws Exception {
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), pattern)
                        .setFileEnumerator(
                                () -> new GlobFileEnumerator(new NonSplittingRecursiveEnumerator()))
                        .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        final List<String> records = new ArrayList<>();
        try (CloseableIterator<String> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "glob-hdfs")
                        .executeAndCollect()) {
            iterator.forEachRemaining(records::add);
        }
        return records;
    }

    private Path path(String suffix) {
        return new Path(cluster.getURI().toString() + suffix);
    }

    private void write(String name, String contents) throws Exception {
        try (FSDataOutputStream out =
                cluster.getFileSystem()
                        .create(new org.apache.hadoop.fs.Path(name), true, 4096, (short) 1, 1024)) {
            out.write(contents.getBytes(StandardCharsets.UTF_8));
        }
    }
}
