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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.committer.FileCommitter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketStateGenerator;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketStatePathResolver;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link FileWriterBucketStateSerializer} that verify we can still read snapshots
 * taken from old {@link StreamingFileSink}. We keep snapshots for all previous versions in version
 * control (including the current version). The tests verify that the current version of the
 * serializer can still read data from all previous versions.
 *
 * <p>This is a mirror of {@link
 * org.apache.flink.streaming.api.functions.sink.filesystem.BucketStateSerializerTest} that verifies
 * we can restore the same snapshots. We therefore have the same "previous versions" as that other
 * test. The generated test data from {@code BucketStateSerializerTest} has been copied to be reused
 * in this test to ensure we can restore the same bytes.
 */
@RunWith(Parameterized.class)
public class FileWriterBucketStateSerializerMigrationTest {

    private static final int CURRENT_VERSION = 2;

    @Parameterized.Parameters(name = "Previous Version = {0}")
    public static Collection<Integer> previousVersions() {
        return Arrays.asList(1, 2);
    }

    @Parameterized.Parameter public Integer previousVersion;

    private static final String IN_PROGRESS_CONTENT = "writing";
    private static final String PENDING_CONTENT = "wrote";

    private static final String BUCKET_ID = "test-bucket";

    private static final java.nio.file.Path BASE_PATH =
            Paths.get("src/test/resources/").resolve("bucket-state-migration-test");

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

    private final BucketStateGenerator generator =
            new BucketStateGenerator(
                    BUCKET_ID, IN_PROGRESS_CONTENT, PENDING_CONTENT, BASE_PATH, CURRENT_VERSION);

    @Test
    @Ignore
    public void prepareDeserializationEmpty() throws IOException {
        generator.prepareDeserializationEmpty();
    }

    @Test
    public void testSerializationEmpty() throws IOException {

        final String scenarioName = "empty";
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, previousVersion);

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);
        final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());
        final FileWriterBucketState recoveredState = readBucketState(scenarioName, previousVersion);

        final FileWriterBucket<String> bucket = restoreBucket(recoveredState);

        Assert.assertEquals(testBucketPath, bucket.getBucketPath());
        Assert.assertNull(bucket.getInProgressPart());
        Assert.assertTrue(bucket.getPendingFiles().isEmpty());
    }

    @Test
    @Ignore
    public void prepareDeserializationOnlyInProgress() throws IOException {
        generator.prepareDeserializationOnlyInProgress();
    }

    @Test
    public void testSerializationOnlyInProgress() throws IOException {

        final String scenarioName = "only-in-progress";
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, previousVersion);

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);

        final Path testBucketPath = new Path(outputPath.resolve(BUCKET_ID).toString());

        final FileWriterBucketState recoveredState = readBucketState(scenarioName, previousVersion);

        final FileWriterBucket<String> bucket = restoreBucket(recoveredState);

        Assert.assertEquals(testBucketPath, bucket.getBucketPath());

        // check restore the correct in progress file writer
        Assert.assertEquals(8, bucket.getInProgressPart().getSize());

        long numFiles =
                Files.list(Paths.get(testBucketPath.toString()))
                        .map(
                                file -> {
                                    assertThat(
                                            file.getFileName().toString(),
                                            startsWith(".part-0-0.inprogress"));
                                    return 1;
                                })
                        .count();

        assertThat(numFiles, is(1L));
    }

    @Test
    @Ignore
    public void prepareDeserializationFull() throws IOException {
        generator.prepareDeserializationFull();
    }

    @Test
    public void testSerializationFull() throws IOException {
        testDeserializationFull(true, "full");
    }

    @Test
    @Ignore
    public void prepareDeserializationNullInProgress() throws IOException {
        generator.prepareDeserializationNullInProgress();
    }

    @Test
    public void testSerializationNullInProgress() throws IOException {
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
            final FileWriterBucketState recoveredState =
                    readBucketStateFromTemplate(scenarioName, previousVersion);
            final int noOfPendingCheckpoints = 5;

            // there are 5 checkpoint does not complete.
            final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
                    pendingFileRecoverables =
                            recoveredState.getPendingFileRecoverablesPerCheckpoint();
            Assert.assertEquals(5L, pendingFileRecoverables.size());

            final Set<String> beforeRestorePaths =
                    Files.list(outputPath.resolve(BUCKET_ID))
                            .map(file -> file.getFileName().toString())
                            .collect(Collectors.toSet());

            // before retsoring all file has "inprogress"
            for (int i = 0; i < noOfPendingCheckpoints; i++) {
                final String part = ".part-0-" + i + ".inprogress";
                assertThat(beforeRestorePaths, hasItem(startsWith(part)));
            }

            final FileWriterBucket<String> bucket = restoreBucket(recoveredState);
            Assert.assertEquals(testBucketPath, bucket.getBucketPath());
            Assert.assertEquals(noOfPendingCheckpoints, bucket.getPendingFiles().size());

            // simulates we commit the recovered pending files on the first checkpoint
            bucket.snapshotState();
            List<FileSinkCommittable> committables = bucket.prepareCommit(false);
            FileCommitter committer = new FileCommitter(createBucketWriter());
            committer.commit(committables);

            final Set<String> afterRestorePaths =
                    Files.list(outputPath.resolve(BUCKET_ID))
                            .map(file -> file.getFileName().toString())
                            .collect(Collectors.toSet());

            // after restoring all pending files are comitted.
            // there is no "inporgress" in file name for the committed files.
            for (int i = 0; i < noOfPendingCheckpoints; i++) {
                final String part = "part-0-" + i;
                assertThat(afterRestorePaths, hasItem(part));
                afterRestorePaths.remove(part);
            }

            if (withInProgress) {
                // only the in-progress must be left
                assertThat(afterRestorePaths, iterableWithSize(1));

                // verify that the in-progress file is still there
                assertThat(
                        afterRestorePaths,
                        hasItem(startsWith(".part-0-" + noOfPendingCheckpoints + ".inprogress")));
            } else {
                assertThat(afterRestorePaths, empty());
            }
        } finally {
            FileUtils.deleteDirectory(pathResolver.getResourcePath(scenarioName).toFile());
        }
    }

    private static FileWriterBucket<String> restoreBucket(final FileWriterBucketState bucketState)
            throws IOException {
        return FileWriterBucket.restore(
                createBucketWriter(),
                DefaultRollingPolicy.builder().withMaxPartSize(10).build(),
                bucketState,
                OutputFileConfig.builder().build());
    }

    private static RowWiseBucketWriter<String, String> createBucketWriter() throws IOException {
        return new RowWiseBucketWriter<>(
                FileSystem.getLocalFileSystem().createRecoverableWriter(),
                new SimpleStringEncoder<>());
    }

    private static SimpleVersionedSerializer<FileWriterBucketState> bucketStateSerializer()
            throws IOException {
        final RowWiseBucketWriter<String, String> bucketWriter = createBucketWriter();
        return new FileWriterBucketStateSerializer(
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                bucketWriter.getProperties().getPendingFileRecoverableSerializer());
    }

    private static FileWriterBucketState readBucketState(
            final String scenarioName, final int version) throws IOException {
        final BucketStatePathResolver pathResolver =
                new BucketStatePathResolver(BASE_PATH, version);
        byte[] bytes = Files.readAllBytes(pathResolver.getSnapshotPath(scenarioName));
        return SimpleVersionedSerialization.readVersionAndDeSerialize(
                bucketStateSerializer(), bytes);
    }

    private static FileWriterBucketState readBucketStateFromTemplate(
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
