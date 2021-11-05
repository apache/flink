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
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/** Test {@link GSResumeRecoverable}. */
@RunWith(Parameterized.class)
public class GSRecoverableFsDataOutputStreamTest {

    @Parameterized.Parameter(value = 0)
    public boolean empty;

    @Parameterized.Parameter(value = 1)
    @Nullable
    public String temporaryBucketName;

    @Parameterized.Parameter(value = 2)
    public int componentObjectCount;

    @Parameterized.Parameter(value = 3)
    public long position;

    @Parameterized.Parameter(value = 4)
    public boolean closed;

    @Parameterized.Parameters(
            name =
                    "empty={0}, temporaryBucketName={1}, componentObjectCount={2}, position={3}, closed={4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {false, null, 0, 32, false},
                    {false, null, 4, 0, true},
                    {false, "temporary-bucket", 0, 32, false},
                    {false, "temporary-bucket", 4, 64, true},
                    {true, null, 0, 0, false},
                    {true, "temporary-bucket", 0, 0, false},
                });
    }

    private Random random;

    private GSFileSystemOptions options;

    private MockBlobStorage blobStorage;

    private ArrayList<UUID> componentObjectIds;

    private GSRecoverableFsDataOutputStream fsDataOutputStream;

    @Before
    public void before() {

        random = new Random(TestUtils.RANDOM_SEED);

        Configuration flinkConfig = new Configuration();
        if (temporaryBucketName != null) {
            flinkConfig.setString("gs.writer.temporary.bucket.name", temporaryBucketName);
        }

        componentObjectIds = new ArrayList<>();
        for (int i = 0; i < componentObjectCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }

        options = new GSFileSystemOptions(flinkConfig);

        blobStorage = new MockBlobStorage();

        if (empty) {
            fsDataOutputStream =
                    new GSRecoverableFsDataOutputStream(
                            blobStorage, options, TestUtils.BLOB_IDENTIFIER);
        } else {
            GSResumeRecoverable resumeRecoverable =
                    new GSResumeRecoverable(
                            TestUtils.BLOB_IDENTIFIER, componentObjectIds, position, closed);
            fsDataOutputStream =
                    new GSRecoverableFsDataOutputStream(blobStorage, options, resumeRecoverable);
        }
    }

    @Test
    public void shouldConstructStream() {
        assertEquals(blobStorage, fsDataOutputStream.storage);
        assertEquals(options, fsDataOutputStream.options);
        assertEquals(TestUtils.BLOB_IDENTIFIER, fsDataOutputStream.finalBlobIdentifier);
        if (empty) {
            assertEquals(0, fsDataOutputStream.position);
            assertFalse(fsDataOutputStream.closed);
            assertEquals(0, fsDataOutputStream.componentObjectIds.size());

        } else {
            assertEquals(position, fsDataOutputStream.position);
            assertEquals(closed, fsDataOutputStream.closed);
            assertArrayEquals(
                    componentObjectIds.toArray(), fsDataOutputStream.componentObjectIds.toArray());
        }
    }

    @Test
    public void shouldReturnPosition() throws IOException {
        assertEquals(position, fsDataOutputStream.getPos());
    }

    private void writeContent(ThrowingRunnable<IOException> write, byte[] expectedContent)
            throws IOException {

        // write the byte, confirm position change and existence of write channel
        assertEquals(position, fsDataOutputStream.position);
        assertNull(fsDataOutputStream.currentWriteChannel);
        write.run();
        assertEquals(position + expectedContent.length, fsDataOutputStream.position);
        assertNotNull(fsDataOutputStream.currentWriteChannel);

        // close and persist. there should be exactly zero blobs before and one after, with this
        // byte value in it
        assertEquals(0, blobStorage.blobs.size());
        fsDataOutputStream.closeForCommit();
        assertEquals(1, blobStorage.blobs.size());
        GSBlobIdentifier blobIdentifier =
                blobStorage.blobs.keySet().toArray(new GSBlobIdentifier[0])[0];
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(blobIdentifier);
        assertNotNull(blobValue);
        assertArrayEquals(expectedContent, blobValue.content);
    }

    private void writeByte() throws IOException {
        writeContent(
                () -> fsDataOutputStream.write(TestUtils.BYTE_VALUE),
                new byte[] {TestUtils.BYTE_VALUE});
    }

    @Test
    public void shouldWriteByte() throws IOException {
        if (closed) {
            assertThrows(IOException.class, this::writeByte);
        } else {
            writeByte();
        }
    }

    private void writeArray() throws IOException {
        byte[] bytes = new byte[64];
        random.nextBytes(bytes);
        writeContent(() -> fsDataOutputStream.write(bytes), bytes);
    }

    @Test
    public void shouldWriteArray() throws IOException {
        if (closed) {
            assertThrows(IOException.class, this::writeArray);
        } else {
            writeArray();
        }
    }

    private void writeArraySlice() throws IOException {
        final int start = 4;
        final int length = 10;
        byte[] bytes = new byte[64];
        random.nextBytes(bytes);
        writeContent(
                () -> fsDataOutputStream.write(bytes, start, length),
                Arrays.copyOfRange(bytes, start, start + length));
    }

    @Test
    public void shouldWriteArraySlice() throws IOException {
        if (closed) {
            assertThrows(IOException.class, this::writeArraySlice);
        } else {
            writeArraySlice();
        }
    }

    @Test
    public void shouldFlush() throws IOException {
        if (!closed) {
            fsDataOutputStream.write(TestUtils.BYTE_VALUE);
            assertNotNull(fsDataOutputStream.currentWriteChannel);
            fsDataOutputStream.flush();
            assertNull(fsDataOutputStream.currentWriteChannel);
        }
    }

    @Test
    public void shouldSync() throws IOException {
        if (!closed) {
            fsDataOutputStream.write(TestUtils.BYTE_VALUE);
            assertNotNull(fsDataOutputStream.currentWriteChannel);
            fsDataOutputStream.sync();
            assertNull(fsDataOutputStream.currentWriteChannel);
        }
    }

    @Test
    public void shouldPersist() throws IOException {
        if (!closed) {
            GSResumeRecoverable recoverable = (GSResumeRecoverable) fsDataOutputStream.persist();
            assertNull(fsDataOutputStream.currentWriteChannel);
            assertEquals(TestUtils.BLOB_IDENTIFIER, recoverable.finalBlobIdentifier);
            if (empty) {
                assertEquals(0, recoverable.componentObjectIds.size());
            } else {
                assertArrayEquals(
                        componentObjectIds.toArray(), recoverable.componentObjectIds.toArray());
            }
            assertEquals(position, recoverable.position);
            assertFalse(recoverable.closed);
        }
    }

    @Test
    public void shouldFailOnPartialWrite() throws IOException {
        if (!closed) {
            blobStorage.maxWriteCount = 1;
            byte[] bytes = new byte[2];
            random.nextBytes(bytes);

            assertThrows(IOException.class, () -> fsDataOutputStream.write(bytes));
        }
    }
}
