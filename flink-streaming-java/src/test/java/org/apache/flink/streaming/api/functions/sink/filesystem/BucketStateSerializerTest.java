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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link BucketStateSerializer} that verify we can still read snapshots written using
 * an older version of the serializer. We keep snapshots for all previous versions in version
 * control (including the current version). The tests verify that the current version of the
 * serializer can still read data from all previous versions.
 */
@ExtendWith(ParameterizedTestExtension.class)
class BucketStateSerializerTest {

    private static final int CURRENT_VERSION = 2;

    @Parameters(name = "Previous Version = {0}")
    private static Collection<Integer> previousVersions() {
        return Arrays.asList(1, 2);
    }

    @Parameter private Integer previousVersion;

    private static final String IN_PROGRESS_CONTENT = "writing";
    private static final String PENDING_CONTENT = "wrote";

    private static final String BUCKET_ID = "test-bucket";

    private static final java.nio.file.Path BASE_PATH =
            Paths.get("src/test/resources/").resolve("bucket-state-migration-test");

    private final BucketStateGenerator generator =
            new BucketStateGenerator(
                    BUCKET_ID, IN_PROGRESS_CONTENT, PENDING_CONTENT, BASE_PATH, CURRENT_VERSION);

    @TestTemplate
    @Disabled
    void prepareDeserializationEmpty() throws IOException {
        generator.prepareDeserializationEmpty();
    }

    @TestTemplate
    void testSerializationEmpty() throws IOException {

        final String scenarioName = "empty";
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, previousVersion);

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);
        final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());
        final BucketState<String> recoveredState = readBucketState(scenarioName, previousVersion);

        final Bucket<String, String> bucket = restoreBucket(0, recoveredState);

        assertThat(bucket.getBucketPath()).isEqualTo(testBucketPath);
        assertThat(bucket.getInProgressPart()).isNull();
        assertThat(bucket.getPendingFileRecoverablesPerCheckpoint()).isEmpty();
    }

    @TestTemplate
    @Disabled
    void prepareDeserializationOnlyInProgress() throws IOException {
        generator.prepareDeserializationOnlyInProgress();
    }

    @TestTemplate
    void testSerializationOnlyInProgress() throws IOException {

        final String scenarioName = "only-in-progress";
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, previousVersion);

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);

        final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

        final BucketState<String> recoveredState = readBucketState(scenarioName, previousVersion);

        final Bucket<String, String> bucket = restoreBucket(0, recoveredState);

        assertThat(bucket.getBucketPath()).isEqualTo(testBucketPath);

        // check restore the correct in progress file writer
        assertThat(bucket.getInProgressPart().getSize()).isEqualTo(8);

        long numFiles =
                Files.list(Paths.get(testBucketPath.toString()))
                        .map(
                                file -> {
                                    assertThat(file.getFileName().toString())
                                            .startsWith(".part-0-0.inprogress");
                                    return 1;
                                })
                        .count();

        assertThat(numFiles).isOne();
    }

    @TestTemplate
    @Disabled
    void prepareDeserializationFull() throws IOException {
        generator.prepareDeserializationFull();
    }

    @TestTemplate
    void testSerializationFull() throws IOException {
        testDeserializationFull(true, "full");
    }

    @TestTemplate
    @Disabled
    public void prepareDeserializationNullInProgress() throws IOException {
        generator.prepareDeserializationNullInProgress();
    }

    @TestTemplate
    void testSerializationNullInProgress() throws IOException {
        testDeserializationFull(false, "full-no-in-progress");
    }

    private void testDeserializationFull(final boolean withInProgress, final String scenarioName)
            throws IOException {

        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, previousVersion);

        try {
            final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);
            final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());
            // restore the state
            final BucketState<String> recoveredState =
                    readBucketStateFromTemplate(scenarioName, previousVersion);
            final int noOfPendingCheckpoints = 5;

            // there are 5 checkpoint does not complete.
            final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
                    pendingFileRecoverables =
                            recoveredState.getPendingFileRecoverablesPerCheckpoint();
            assertThat(pendingFileRecoverables).hasSize(5);

            final Set<String> beforeRestorePaths =
                    Files.list(outputPath.resolve(BUCKET_ID))
                            .map(file -> file.getFileName().toString())
                            .collect(Collectors.toSet());

            // before retsoring all file has "inprogress"
            for (int i = 0; i < noOfPendingCheckpoints; i++) {
                final String part = ".part-0-" + i + ".inprogress";
                assertThat(beforeRestorePaths).anyMatch(item -> item.startsWith(part));
            }

            // recover and commit
            final Bucket bucket = restoreBucket(noOfPendingCheckpoints + 1, recoveredState);
            assertThat(bucket.getBucketPath()).isEqualTo(testBucketPath);
            assertThat(bucket.getPendingFileRecoverablesForCurrentCheckpoint()).isEmpty();

            final Set<String> afterRestorePaths =
                    Files.list(outputPath.resolve(BUCKET_ID))
                            .map(file -> file.getFileName().toString())
                            .collect(Collectors.toSet());

            // after restoring all pending files are comitted.
            // there is no "inporgress" in file name for the committed files.
            for (int i = 0; i < noOfPendingCheckpoints; i++) {
                final String part = "part-0-" + i;
                assertThat(afterRestorePaths).contains(part);
                afterRestorePaths.remove(part);
            }

            if (withInProgress) {
                // only the in-progress must be left
                assertThat(afterRestorePaths).hasSize(1);

                // verify that the in-progress file is still there
                assertThat(afterRestorePaths)
                        .anyMatch(
                                item ->
                                        item.startsWith(
                                                ".part-0-"
                                                        + noOfPendingCheckpoints
                                                        + ".inprogress"));
            } else {
                assertThat(afterRestorePaths).isEmpty();
            }
        } finally {
            FileUtils.deleteDirectory(pathResolver.getResourcePath(scenarioName).toFile());
        }
    }

    private static Bucket<String, String> restoreBucket(
            final int initialPartCounter, final BucketState<String> bucketState)
            throws IOException {
        return Bucket.restore(
                0,
                initialPartCounter,
                createBucketWriter(),
                DefaultRollingPolicy.builder().withMaxPartSize(new MemorySize(10)).build(),
                bucketState,
                null,
                OutputFileConfig.builder().build());
    }

    private static RowWiseBucketWriter<String, String> createBucketWriter() throws IOException {
        return new RowWiseBucketWriter<>(
                FileSystem.getLocalFileSystem().createRecoverableWriter(),
                new SimpleStringEncoder<>());
    }

    private static SimpleVersionedSerializer<BucketState<String>> bucketStateSerializer()
            throws IOException {
        final RowWiseBucketWriter<String, String> bucketWriter = createBucketWriter();
        return new BucketStateSerializer<>(
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                SimpleVersionedStringSerializer.INSTANCE);
    }

    private static BucketState<String> readBucketState(final String scenarioName, final int version)
            throws IOException {
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, version);
        byte[] bytes = Files.readAllBytes(pathResolver.getSnapshotPath(scenarioName));
        return SimpleVersionedSerialization.readVersionAndDeSerialize(
                bucketStateSerializer(), bytes);
    }

    private static BucketState<String> readBucketStateFromTemplate(
            final String scenarioName, final int version) throws IOException {
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, version);
        final java.nio.file.Path scenarioPath = pathResolver.getResourcePath(scenarioName);

        // clear the scenario files first
        FileUtils.deleteDirectory(scenarioPath.toFile());

        // prepare the scenario files
        FileUtils.copy(
                new Path(scenarioPath.toString() + "-template"),
                new Path(scenarioPath.toString()),
                false);

        return readBucketState(scenarioName, version);
    }
}
