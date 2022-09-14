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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests that read the BoundedBlockingSubpartition with multiple threads in parallel. */
@RunWith(Parameterized.class)
public abstract class BoundedDataTestBase {

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    /**
     * Max buffer sized used by the tests that write data. For implementations that need to
     * instantiate buffers in the read side.
     */
    protected static final int BUFFER_SIZE = 1024 * 1024; // 1 MiByte

    private static final String COMPRESSION_CODEC = "LZ4";

    private static final BufferCompressor COMPRESSOR =
            new BufferCompressor(BUFFER_SIZE, COMPRESSION_CODEC);

    private static final BufferDecompressor DECOMPRESSOR =
            new BufferDecompressor(BUFFER_SIZE, COMPRESSION_CODEC);

    @Parameterized.Parameter public static boolean compressionEnabled;

    @Parameterized.Parameters(name = "compressionEnabled = {0}")
    public static Boolean[] compressionEnabled() {
        return new Boolean[] {false, true};
    }

    // ------------------------------------------------------------------------
    //  BoundedData Instantiation
    // ------------------------------------------------------------------------

    protected abstract boolean isRegionBased();

    protected abstract BoundedData createBoundedData(Path tempFilePath) throws IOException;

    protected abstract BoundedData createBoundedDataWithRegion(Path tempFilePath, int regionSize)
            throws IOException;

    protected BoundedData createBoundedData() throws IOException {
        return createBoundedData(createTempPath());
    }

    private BoundedData createBoundedDataWithRegion(int regionSize) throws IOException {
        return createBoundedDataWithRegion(createTempPath(), regionSize);
    }

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    @Test
    public void testWriteAndReadData() throws Exception {
        try (BoundedData bd = createBoundedData()) {
            testWriteAndReadData(bd);
        }
    }

    @Test
    public void testWriteAndReadDataAcrossRegions() throws Exception {
        if (!isRegionBased()) {
            return;
        }

        try (BoundedData bd = createBoundedDataWithRegion(1_276_347)) {
            testWriteAndReadData(bd);
        }
    }

    private void testWriteAndReadData(BoundedData bd) throws Exception {
        final int numLongs = 10_000_000;
        final int numBuffers = writeLongs(bd, numLongs);
        bd.finishWrite();

        readLongs(bd.createReader(), numBuffers, numLongs);
    }

    @Test
    public void returnNullAfterEmpty() throws Exception {
        try (BoundedData bd = createBoundedData()) {
            bd.finishWrite();

            final BoundedData.Reader reader = bd.createReader();

            // check that multiple calls now return empty buffers
            assertNull(reader.nextBuffer());
            assertNull(reader.nextBuffer());
            assertNull(reader.nextBuffer());
        }
    }

    @Test
    public void testDeleteFileOnClose() throws Exception {
        final Path path = createTempPath();
        final BoundedData bd = createBoundedData(path);
        assertTrue(Files.exists(path));

        bd.close();

        assertFalse(Files.exists(path));
    }

    @Test
    public void testGetSizeSingleRegion() throws Exception {
        try (BoundedData bd = createBoundedData()) {
            testGetSize(bd, 60_787, 76_687);
        }
    }

    @Test
    public void testGetSizeMultipleRegions() throws Exception {
        if (!isRegionBased()) {
            return;
        }

        int pageSize = PageSizeUtil.getSystemPageSizeOrConservativeMultiple();
        try (BoundedData bd = createBoundedDataWithRegion(pageSize)) {
            testGetSize(bd, pageSize / 3, pageSize - BufferReaderWriterUtil.HEADER_LENGTH);
        }
    }

    private static void testGetSize(BoundedData bd, int bufferSize1, int bufferSize2)
            throws Exception {
        final int expectedSize1 = bufferSize1 + BufferReaderWriterUtil.HEADER_LENGTH;
        final int expectedSizeFinal =
                bufferSize1 + bufferSize2 + 2 * BufferReaderWriterUtil.HEADER_LENGTH;

        bd.writeBuffer(BufferBuilderTestUtils.buildSomeBuffer(bufferSize1));
        assertEquals(expectedSize1, bd.getSize());

        bd.writeBuffer(BufferBuilderTestUtils.buildSomeBuffer(bufferSize2));
        assertEquals(expectedSizeFinal, bd.getSize());

        bd.finishWrite();
        assertEquals(expectedSizeFinal, bd.getSize());
    }

    // ------------------------------------------------------------------------
    //  utils
    // ------------------------------------------------------------------------

    private static int writeLongs(BoundedData bd, int numLongs) throws IOException {
        final int numLongsInBuffer = BUFFER_SIZE / Long.BYTES;
        int numBuffers = 0;

        for (long nextValue = 0; nextValue < numLongs; nextValue += numLongsInBuffer) {
            Buffer buffer =
                    BufferBuilderTestUtils.buildBufferWithAscendingLongs(
                            BUFFER_SIZE, numLongsInBuffer, nextValue);
            if (compressionEnabled) {
                Buffer compressedBuffer = COMPRESSOR.compressToIntermediateBuffer(buffer);
                bd.writeBuffer(compressedBuffer);
                // recycle intermediate buffer.
                if (compressedBuffer != buffer) {
                    compressedBuffer.recycleBuffer();
                }
            } else {
                bd.writeBuffer(buffer);
            }
            numBuffers++;
            buffer.recycleBuffer();
        }

        return numBuffers;
    }

    private static void readLongs(BoundedData.Reader reader, int numBuffersExpected, int numLongs)
            throws IOException {
        Buffer b;
        long nextValue = 0;
        int numBuffers = 0;

        int numLongsInBuffer;
        while ((b = reader.nextBuffer()) != null) {
            if (compressionEnabled && b.isCompressed()) {
                Buffer decompressedBuffer = DECOMPRESSOR.decompressToIntermediateBuffer(b);
                numLongsInBuffer = decompressedBuffer.getSize() / Long.BYTES;
                BufferBuilderTestUtils.validateBufferWithAscendingLongs(
                        decompressedBuffer, numLongsInBuffer, nextValue);
                // recycle intermediate buffer.
                decompressedBuffer.recycleBuffer();
            } else {
                numLongsInBuffer = b.getSize() / Long.BYTES;
                BufferBuilderTestUtils.validateBufferWithAscendingLongs(
                        b, numLongsInBuffer, nextValue);
            }
            nextValue += numLongsInBuffer;
            numBuffers++;

            b.recycleBuffer();
        }

        assertEquals(numBuffersExpected, numBuffers);
        assertThat(nextValue, Matchers.greaterThanOrEqualTo((long) numLongs));
    }

    private static Path createTempPath() throws IOException {
        return new File(TMP_FOLDER.newFolder(), "subpartitiondata").toPath();
    }
}
