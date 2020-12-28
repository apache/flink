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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Utilities to generate sample bucket states so that we could test migration of bucket states. */
public class BucketStateGenerator {
    private final String bucketId;
    private final String inProgressContent;
    private final String pendingContent;
    private final BucketStatePathResolver pathResolver;

    public BucketStateGenerator(
            String bucketId,
            String inProgressContent,
            String pendingContent,
            java.nio.file.Path basePath,
            int currentVersion) {
        this.bucketId = bucketId;
        this.inProgressContent = inProgressContent;
        this.pendingContent = pendingContent;
        this.pathResolver = new BucketStatePathResolver(basePath, currentVersion);
    }

    public void prepareDeserializationEmpty() throws IOException {
        final String scenarioName = "empty";
        final java.nio.file.Path scenarioPath = pathResolver.getResourcePath(scenarioName);

        FileUtils.deleteDirectory(scenarioPath.toFile());
        Files.createDirectories(scenarioPath);

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);
        final Path testBucketPath = new Path(outputPath.resolve(bucketId).toString());

        final Bucket<String, String> bucket = createNewBucket(testBucketPath);

        final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(0);

        byte[] bytes =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        bucketStateSerializer(), bucketState);
        Files.write(pathResolver.getSnapshotPath(scenarioName), bytes);
    }

    public void prepareDeserializationOnlyInProgress() throws IOException {
        final String scenarioName = "only-in-progress";
        final java.nio.file.Path scenarioPath = pathResolver.getResourcePath(scenarioName);
        FileUtils.deleteDirectory(scenarioPath.toFile());
        Files.createDirectories(scenarioPath);

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);
        final Path testBucketPath = new Path(outputPath.resolve(bucketId).toString());

        final Bucket<String, String> bucket = createNewBucket(testBucketPath);

        bucket.write(inProgressContent, System.currentTimeMillis());

        final BucketState<String> bucketState = bucket.onReceptionOfCheckpoint(0);

        final byte[] bytes =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        bucketStateSerializer(), bucketState);

        Files.write(pathResolver.getSnapshotPath(scenarioName), bytes);
    }

    public void prepareDeserializationFull() throws IOException {
        prepareDeserializationFull(true, "full");
    }

    public void prepareDeserializationNullInProgress() throws IOException {
        prepareDeserializationFull(false, "full-no-in-progress");
    }

    private void prepareDeserializationFull(final boolean withInProgress, final String scenarioName)
            throws IOException {
        final java.nio.file.Path scenarioPath = pathResolver.getResourcePath(scenarioName);
        FileUtils.deleteDirectory(Paths.get(scenarioPath.toString() + "-template").toFile());
        Files.createDirectories(scenarioPath);

        final int noOfPendingCheckpoints = 5;

        final java.nio.file.Path outputPath = pathResolver.getOutputPath(scenarioName);

        final Path testBucketPath = new Path(outputPath.resolve(bucketId).toString());

        final Bucket<String, String> bucket = createNewBucket(testBucketPath);

        BucketState<String> bucketState = null;
        // pending for checkpoints
        for (int i = 0; i < noOfPendingCheckpoints; i++) {
            // write 10 bytes to the in progress file
            bucket.write(pendingContent, System.currentTimeMillis());
            bucket.write(pendingContent, System.currentTimeMillis());
            // every checkpoint would produce a pending file
            bucketState = bucket.onReceptionOfCheckpoint(i);
        }

        if (withInProgress) {
            // create a in progress file
            bucket.write(inProgressContent, System.currentTimeMillis());

            // 5 pending files and 1 in progress file
            bucketState = bucket.onReceptionOfCheckpoint(noOfPendingCheckpoints);
        }

        final byte[] bytes =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        bucketStateSerializer(), bucketState);

        Files.write(pathResolver.getSnapshotPath(scenarioName), bytes);

        // copy the scenario file to a template directory.
        // it is because that the test `testSerializationFull` would change the in progress file to
        // pending files.
        moveToTemplateDirectory(scenarioPath);
    }

    private static RowWiseBucketWriter<String, String> createBucketWriter() throws IOException {
        return new RowWiseBucketWriter<>(
                FileSystem.getLocalFileSystem().createRecoverableWriter(),
                new SimpleStringEncoder<>());
    }

    private static SimpleVersionedSerializer<BucketState<String>> bucketStateSerializer()
            throws IOException {
        final RowWiseBucketWriter bucketWriter = createBucketWriter();
        return new BucketStateSerializer<>(
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                SimpleVersionedStringSerializer.INSTANCE);
    }

    private Bucket<String, String> createNewBucket(Path bucketPath) throws IOException {
        return Bucket.getNew(
                0,
                bucketId,
                bucketPath,
                0,
                createBucketWriter(),
                DefaultRollingPolicy.builder().withMaxPartSize(10).build(),
                null,
                OutputFileConfig.builder().build());
    }

    private void moveToTemplateDirectory(java.nio.file.Path scenarioPath) throws IOException {
        FileUtils.copy(
                new Path(scenarioPath.toString()),
                new Path(scenarioPath.toString() + "-template"),
                false);
        FileUtils.deleteDirectory(scenarioPath.toFile());
    }
}
