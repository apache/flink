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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.TestUtils;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.MockBlobStorage;
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link GSRecoverableWriterCommitter}. */
@ExtendWith(ParameterizedTestExtension.class)
class GSRecoverableWriterCommitterTest {

    @Parameter @Nullable private String temporaryBucketName;

    @Parameter(value = 1)
    private int composeMaxBlobs;

    @Parameter(value = 2)
    private int[] blobSizes;

    @Parameter(value = 3)
    private int commitBlobCount;

    @Parameters(
            name =
                    "temporaryBucketName={0}, composeMaxBlobs={1}, blobSizes={2}, commitBlobCount={3}")
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // no specified temporary bucket, compose up to 4 blobs at once, compose no
                    // blobs; commit after 0 blobs
                    {null, 4, new int[] {}, 0},
                    // no specified temporary bucket, compose up to 4 blobs at once, compose blob of
                    // size 64; commit after 1 blobs
                    {null, 4, new int[] {64}, 1},
                    // no specified temporary bucket, compose up to 4 blobs at once, compose blob of
                    // size 64, 128, and 96; commit after 2 blobs
                    {null, 4, new int[] {64, 128, 96}, 2},
                    // no specified temporary bucket, compose up to 4 blobs at once, compose blob of
                    // size 64, 128, 96, 32, 256, and 128; commit after 5 blobs
                    {null, 4, new int[] {64, 128, 96, 32, 256, 128}, 5},
                    // specified temporary bucket, compose up to 4 blobs at once, compose no blobs;
                    // commit after 0 blobs
                    {"temporary-bucket", 4, new int[] {}, 0},
                    // specified temporary bucket, compose up to 4 blobs at once, compose blobs of
                    // size 64; commit after 1 blobs
                    {"temporary-bucket", 4, new int[] {64}, 1},
                    // specified temporary bucket, compose up to 4 blobs at once, compose blobs of
                    // size 64, 128, and 96; commit after 2 blobs
                    {"temporary-bucket", 4, new int[] {64, 128, 96}, 2},
                    // specified temporary bucket, compose up to 4 blobs at once, compose blobs of
                    // size 64, 128, 96, 32, 256, and 128; commit after 5 blobs
                    {"temporary-bucket", 4, new int[] {64, 128, 96, 32, 256, 128}, 5},
                });
    }

    private GSFileSystemOptions options;

    private Random random;

    private MockBlobStorage blobStorage;

    private ByteArrayOutputStream expectedBytes;

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    void before() {
        Configuration flinkConfig = new Configuration();
        if (temporaryBucketName != null) {
            flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, temporaryBucketName);
        }
        options = new GSFileSystemOptions(flinkConfig);

        random = new Random();
        random.setSeed(TestUtils.RANDOM_SEED);

        blobStorage = new MockBlobStorage();
        blobIdentifier = new GSBlobIdentifier("foo", "bar");

        expectedBytes = new ByteArrayOutputStream();
    }

    @AfterEach
    void after() throws IOException {
        expectedBytes.close();
    }

    /**
     * Test writing a blob.
     *
     * @throws IOException On underlying failure
     */
    @TestTemplate
    void commitTest() throws IOException {
        GSRecoverableWriterCommitter committer = commitTestInternal();
        committer.commit();

        // there should be exactly one blob left, the final blob identifier. validate its contents.
        assertThat(blobStorage.blobs).hasSize(1);
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(blobIdentifier);
        assertThat(blobValue).isNotNull();
        assertThat(blobValue.content).isEqualTo(expectedBytes.toByteArray());
    }

    /**
     * Tests committing a blob where the target blob already exists, this should fail.
     *
     * @throws IOException On underlying failure
     */
    @TestTemplate
    void commitOverwriteShouldFailTest() throws IOException {
        blobStorage.createBlob(blobIdentifier);
        GSRecoverableWriterCommitter committer = commitTestInternal();

        assertThatThrownBy(() -> committer.commit()).isInstanceOf(IOException.class);
    }

    /**
     * Tests committing a blob after recovery where the target blob already exists, this should
     * succeed.
     *
     * @throws IOException On underlying failure
     */
    @TestTemplate
    void commitWithRecoveryOverwriteShouldSucceedTest() throws IOException {
        blobStorage.createBlob(blobIdentifier);
        GSRecoverableWriterCommitter committer = commitTestInternal();
        committer.commitAfterRecovery();
    }

    /**
     * Internal commit function called by other tests. Writes some number of blobs, creates a commit
     * recoverable after some number of them (possibly not all of them!), and then commits.
     *
     * @return The committer
     * @throws IOException On underlying failure
     */
    private GSRecoverableWriterCommitter commitTestInternal() throws IOException {

        // this will hold the component object ids to commit
        ArrayList<UUID> componentObjectIdsToCommit = new ArrayList<>();

        // create the blobs
        for (int blobIndex = 0; blobIndex < blobSizes.length; blobIndex++) {

            // create the object id and blob identifier
            UUID componentObjectId = UUID.randomUUID();
            GSBlobIdentifier temporaryBlobIdentifier =
                    BlobUtils.getTemporaryBlobIdentifier(
                            blobIdentifier, componentObjectId, options);

            // write the bytes to mock storage
            int blobSize = blobSizes[blobIndex];
            byte[] bytes = new byte[blobSize];
            random.nextBytes(bytes);
            blobStorage.blobs.put(temporaryBlobIdentifier, new MockBlobStorage.BlobValue(bytes));

            // will we commit this blob? if so, record that we will use this component object id,
            // that we expect to see the associated bytes in the result
            if (blobIndex <= commitBlobCount) {
                componentObjectIdsToCommit.add(componentObjectId);
                expectedBytes.write(bytes);
            }
        }

        // create the recoverable and commit
        GSCommitRecoverable recoverable =
                new GSCommitRecoverable(blobIdentifier, componentObjectIdsToCommit);
        return new GSRecoverableWriterCommitter(blobStorage, options, recoverable, composeMaxBlobs);
    }
}
