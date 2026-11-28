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

package org.apache.flink.fs.s3native;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.enumerate.GlobFileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** FileSource glob reading through the native S3 plugin and an actual S3-compatible service. */
@Timeout(120)
class FileSourceS3GlobITCase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            SEAWEEDFS_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsNativeS3TestContainer::new));

    @BeforeAll
    static void initializeFileSystem() {
        final Configuration config = new Configuration();
        container().setS3ConfigOptions(config);
        FileSystem.initialize(config, null);
    }

    @AfterAll
    static void resetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Test
    void testBoundedReadingAndNoMatches() throws Exception {
        final String prefix = UUID.randomUUID().toString();
        put(prefix + "/part-1/file-a.txt", "first\n");
        put(prefix + "/part-2/file-b.txt", "second\n");
        put(prefix + "/part-10/file-c.txt", "unselected\n");
        put(prefix + "/archive/part-1/file-d.txt", "unselected\n");
        put(prefix + "/part-1/_hidden.txt", "hidden\n");

        assertThat(path(prefix).getFileSystem()).isInstanceOf(NativeS3FileSystem.class);
        assertThat(read(source(prefix + "/part-[12]/file-?.txt").build()))
                .containsExactlyInAnyOrder("first", "second");
        assertThat(read(source(prefix + "/missing/*.txt").build())).isEmpty();
    }

    @Test
    void testSpecialKeysRecursiveAndOverlappingPatterns() throws Exception {
        final String prefix = UUID.randomUUID().toString();
        put(prefix + "/{literal}/report[2026].txt", "bracket\n");
        put(prefix + "/{literal}/space % #+.txt", "encoded\n");
        put(prefix + "/{literal}/deep/nested.txt", "nested\n");
        put(prefix + "/{literal}/_hidden.txt", "hidden\n");
        put(prefix + "/{literal}/.hidden/visible.txt", "hidden\n");

        assertThat(read(source(prefix + "/{literal}/report[[]2026].txt").build()))
                .containsExactly("bracket");
        assertThat(read(source(prefix + "/{literal}/space % #+.*").build()))
                .containsExactly("encoded");
        final FileSource<String> overlapping =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                path(prefix + "/{literal}/**/*.txt"),
                                path(prefix + "/{literal}/deep"))
                        .setFileEnumerator(
                                () -> new GlobFileEnumerator(new NonSplittingRecursiveEnumerator()))
                        .build();
        assertThat(read(overlapping)).containsExactlyInAnyOrder("bracket", "encoded", "nested");
    }

    @Test
    void testContinuousDiscoveryOfNewPartitions() throws Exception {
        final String prefix = UUID.randomUUID().toString();
        final FileSource<String> source =
                source(prefix + "/part-*/*.txt").monitorContinuously(Duration.ofMillis(20)).build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "glob-s3");
        try (CloseableIterator<String> records = stream.collectAsync()) {
            final JobClient job = env.executeAsync("S3 glob continuous discovery");
            try {
                put(prefix + "/part-1/first.txt", "first\n");
                assertThat(records.next()).isEqualTo("first");
                put(prefix + "/part-2/second.txt", "second\n");
                assertThat(records.next()).isEqualTo("second");
            } finally {
                job.cancel().get();
            }
        }
    }

    private static FileSource.FileSourceBuilder<String> source(String pattern) {
        return FileSource.forRecordStreamFormat(new TextLineInputFormat(), path(pattern))
                .setFileEnumerator(
                        () -> new GlobFileEnumerator(new NonSplittingRecursiveEnumerator()));
    }

    private static List<String> read(FileSource<String> source) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        final List<String> records = new ArrayList<>();
        try (CloseableIterator<String> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "glob-s3")
                        .executeAndCollect()) {
            iterator.forEachRemaining(records::add);
        }
        return records;
    }

    private static void put(String key, String text) {
        container()
                .getClient()
                .putObject(
                        request -> request.bucket(container().getDefaultBucketName()).key(key),
                        RequestBody.fromString(text));
    }

    private static Path path(String key) {
        return new Path(container().getS3UriForDefaultBucket() + "/" + key);
    }

    private static SeaweedFsNativeS3TestContainer container() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }
}
