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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test {@link GSRecoverableWriterCommitter}. */
@RunWith(Parameterized.class)
public class GSRecoverableWriterCommitterTest {

    @Parameterized.Parameter(value = 0)
    @Nullable
    public String temporaryBucketName;

    @Parameterized.Parameter(value = 1)
    public int composeMaxBlobs;

    @Parameterized.Parameter(value = 2)
    public int[] blobSizes;

    @Parameterized.Parameter(value = 3)
    public int commitBlobCount;

    @Parameterized.Parameters(
            name = "temporaryBucketName={0}, composeMaxBlobs={1}, blobSizes={2}, commitCount={3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {null, 4, new int[] {}, 0},
                    {null, 4, new int[] {64}, 1},
                    {null, 4, new int[] {64, 128, 96}, 2},
                    {null, 4, new int[] {64, 128, 96, 32, 256, 128}, 5},
                    {"temporary-bucket", 4, new int[] {}, 0},
                    {"temporary-bucket", 4, new int[] {64}, 1},
                    {"temporary-bucket", 4, new int[] {64, 128, 96}, 2},
                    {"temporary-bucket", 4, new int[] {64, 128, 96, 32, 256, 128}, 5},
                });
    }

    private GSFileSystemOptions options;

    private Random random;

    private MockBlobStorage blobStorage;

    private ByteArrayOutputStream expectedBytes;

    @Before
    public void before() {
        Configuration flinkConfig = new Configuration();
        if (temporaryBucketName != null) {
            flinkConfig.setString("gs.writer.temporary.bucket.name", temporaryBucketName);
        }
        options = new GSFileSystemOptions(flinkConfig);

        random = new Random();
        random.setSeed(TestUtils.RANDOM_SEED);

        blobStorage = new MockBlobStorage();

        expectedBytes = new ByteArrayOutputStream();
    }

    @After
    public void after() throws IOException {
        expectedBytes.close();
    }

    /**
     * Test writing a blob.
     *
     * @throws IOException On underlying failure
     */
    @Test
    public void commitTest() throws IOException {
        GSRecoverableWriterCommitter committer = commitTestInternal();
        committer.commit();

        // there should be exactly one blob left, the final blob identifier. validate its contents.
        assertEquals(1, blobStorage.blobs.size());
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(TestUtils.BLOB_IDENTIFIER);
        assertNotNull(blobValue);
        assertArrayEquals(expectedBytes.toByteArray(), blobValue.content);
    }

    /**
     * Tests committing a blob where the target blob already exists, this should fail.
     *
     * @throws IOException On underlying failure
     */
    @Test(expected = IOException.class)
    public void commitOverwriteShouldFailTest() throws IOException {
        blobStorage.createBlob(TestUtils.BLOB_IDENTIFIER);
        GSRecoverableWriterCommitter committer = commitTestInternal();
        committer.commit();
    }

    /**
     * Tests committing a blob after recovery where the target blob already exists, this should
     * succeed.
     *
     * @throws IOException On underlying failure
     */
    @Test
    public void commitWithRecoveryOverwriteShouldSucceedTest() throws IOException {
        blobStorage.createBlob(TestUtils.BLOB_IDENTIFIER);
        GSRecoverableWriterCommitter committer = commitTestInternal();
        committer.commitAfterRecovery();
    }

    /**
     * Internal commit function called by other tests. Writes some number of blobs, creates a commit
     * recoverable after some number of them, and then commits.
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
            GSBlobIdentifier blobIdentifier =
                    BlobUtils.getTemporaryBlobIdentifier(
                            TestUtils.BLOB_IDENTIFIER, componentObjectId, options);

            // write the bytes to mock storage
            int blobSize = blobSizes[blobIndex];
            byte[] bytes = new byte[blobSize];
            random.nextBytes(bytes);
            blobStorage.blobs.put(blobIdentifier, new MockBlobStorage.BlobValue(bytes));

            // will we commit this blob? if so, record that we will use this component object id,
            // that we expect to see the associated bytes in the result
            if (blobIndex <= commitBlobCount) {
                componentObjectIdsToCommit.add(componentObjectId);
                expectedBytes.write(bytes);
            }
        }

        // create the recoverable and commit
        GSCommitRecoverable recoverable =
                new GSCommitRecoverable(TestUtils.BLOB_IDENTIFIER, componentObjectIdsToCommit);
        GSRecoverableWriterCommitter committer =
                new GSRecoverableWriterCommitter(
                        blobStorage, options, recoverable, composeMaxBlobs);
        return committer;

        /*
        committable.commit(committer);

        // there should be exactly one blob left, the final blob identifier. validate its contents.
        assertEquals(1, blobStorage.blobs.size());
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(TestUtils.BLOB_IDENTIFIER);
        assertNotNull(blobValue);
        assertArrayEquals(expectedBytes.toByteArray(), blobValue.content);
         */
    }
}
