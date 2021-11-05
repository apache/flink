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

import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.storage.MockBlobStorage;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.flink.fs.gs.TestUtils.BLOB_IDENTIFIER;
import static org.apache.flink.fs.gs.TestUtils.RANDOM_SEED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test {@link GSChecksumWriteChannel}. */
@RunWith(Parameterized.class)
public class GSChecksumWriteChannelTest {

    /* The sizes of each buffer of bytes used for writing. */
    @Parameterized.Parameter(value = 0)
    public int[] writeSizes;

    /* The start positions in the write buffers. */
    @Parameterized.Parameter(value = 1)
    public int[] writeStarts;

    /* The length of each write. */
    @Parameterized.Parameter(value = 2)
    public int[] writeLengths;

    @Parameterized.Parameter(value = 3)
    public String description;

    @Parameterized.Parameters(name = "{3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {new int[] {64}, new int[] {0}, new int[] {64}, "simple write"},
                    {
                        new int[] {64, 128, 64},
                        new int[] {0, 0, 0},
                        new int[] {64, 128, 64},
                        "multiple write"
                    },
                    {
                        new int[] {64, 128, 64},
                        new int[] {16, 32, 32},
                        new int[] {32, 48, 1},
                        "multiple partial writes"
                    },
                });
    }

    private byte[][] byteBuffers;

    private byte[] expectedWrittenBytes;

    @Before
    public void before() throws IOException {
        Random random = new Random();
        random.setSeed(RANDOM_SEED);

        // initialize the byte buffers and determine what we expect to be written
        byteBuffers = new byte[writeSizes.length][];
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

            for (int i = 0; i < writeSizes.length; i++) {

                int size = writeSizes[i];
                byteBuffers[i] = new byte[size];
                random.nextBytes(byteBuffers[i]);

                int start = writeStarts[i];
                int length = writeLengths[i];
                stream.write(byteBuffers[i], start, length);
            }

            stream.flush();
            expectedWrittenBytes = stream.toByteArray();
        }
    }

    /**
     * Write each of the partial byte buffers and confirm we get the expected results, including a
     * valid checksum the expected data in the storage.
     *
     * @throws IOException On storage failure.
     */
    @Test
    public void shouldWriteProperly() throws IOException {

        MockBlobStorage blobStorage = new MockBlobStorage();
        GSBlobStorage.WriteChannel writeChannel = blobStorage.writeBlob(BLOB_IDENTIFIER);
        GSChecksumWriteChannel checksumWriteChannel =
                new GSChecksumWriteChannel(blobStorage, writeChannel, BLOB_IDENTIFIER);

        // write each partial buffer and validate the written count
        for (int i = 0; i < byteBuffers.length; i++) {
            int writtenCount =
                    checksumWriteChannel.write(byteBuffers[i], writeStarts[i], writeLengths[i]);
            assertEquals(writeLengths[i], writtenCount);
        }

        // close the write, this also validates the checksum
        checksumWriteChannel.close();

        // read the value out of storage, the bytes should match
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(BLOB_IDENTIFIER);
        assertArrayEquals(expectedWrittenBytes, blobValue.content);
    }

    /**
     * Simulate a checksum failure and confirm an exception is thrown.
     *
     * @throws IOException On checksum failure.
     */
    @Test(expected = IOException.class)
    public void shouldThrowOnChecksumMismatch() throws IOException {

        MockBlobStorage blobStorage = new MockBlobStorage();
        blobStorage.forcedChecksum = "";
        GSBlobStorage.WriteChannel writeChannel = blobStorage.writeBlob(BLOB_IDENTIFIER);
        GSChecksumWriteChannel checksumWriteChannel =
                new GSChecksumWriteChannel(blobStorage, writeChannel, BLOB_IDENTIFIER);

        // write each partial buffer and validate the written count
        for (int i = 0; i < byteBuffers.length; i++) {

            int writtenCount =
                    checksumWriteChannel.write(byteBuffers[i], writeStarts[i], writeLengths[i]);
            assertEquals(writeLengths[i], writtenCount);
        }

        // close the write, this also validates the checksum
        checksumWriteChannel.close();
    }
}
