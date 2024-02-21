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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that read the BoundedBlockingSubpartition with multiple threads in parallel. */
@ExtendWith(ParameterizedTestExtension.class)
abstract class BoundedDataTestBase {

    private Path subpartitionDataPath;

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

    @Parameter private static boolean compressionEnabled;

    @Parameters(name = "compressionEnabled = {0}")
    private static List<Boolean> compressionEnabled() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    void before(@TempDir Path tempDir) {
        this.subpartitionDataPath = tempDir.resolve("subpartitiondata");
    }

    // ------------------------------------------------------------------------
    //  BoundedData Instantiation
    // ------------------------------------------------------------------------

    protected abstract boolean isRegionBased();

    protected abstract BoundedData createBoundedData(Path tempFilePath) throws IOException;

    protected abstract BoundedData createBoundedDataWithRegion(Path tempFilePath, int regionSize)
            throws IOException;

    protected BoundedData createBoundedData() throws IOException {
        return createBoundedData(subpartitionDataPath);
    }

    private BoundedData createBoundedDataWithRegion(int regionSize) throws IOException {
        return createBoundedDataWithRegion(subpartitionDataPath, regionSize);
    }

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    @TestTemplate
    void testWriteAndReadData() throws Exception {
        try (BoundedData bd = createBoundedData()) {
            testWriteAndReadData(bd);
        }
    }

    @TestTemplate
    void testWriteAndReadDataAcrossRegions() throws Exception {
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

    @TestTemplate
    void returnNullAfterEmpty() throws Exception {
        try (BoundedData bd = createBoundedData()) {
            bd.finishWrite();

            final BoundedData.Reader reader = bd.createReader();

            // check that multiple calls now return empty buffers
            assertThat(reader.nextBuffer()).isNull();
            assertThat(reader.nextBuffer()).isNull();
            assertThat(reader.nextBuffer()).isNull();
        }
    }

    @TestTemplate
    void testDeleteFileOnClose() throws Exception {
        final BoundedData bd = createBoundedData(subpartitionDataPath);
        assertThat(subpartitionDataPath).exists();

        bd.close();

        assertThat(subpartitionDataPath).doesNotExist();
    }

    @TestTemplate
    void testGetSizeSingleRegion() throws Exception {
        try (BoundedData bd = createBoundedData()) {
            testGetSize(bd, 60_787, 76_687);
        }
    }

    @TestTemplate
    void testGetSizeMultipleRegions() throws Exception {
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
        assertThat(bd.getSize()).isEqualTo(expectedSize1);

        bd.writeBuffer(BufferBuilderTestUtils.buildSomeBuffer(bufferSize2));
        assertThat(bd.getSize()).isEqualTo(expectedSizeFinal);

        bd.finishWrite();
        assertThat(bd.getSize()).isEqualTo(expectedSizeFinal);
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

        assertThat(numBuffers).isEqualTo(numBuffersExpected);
        assertThat(nextValue).isGreaterThanOrEqualTo(numLongs);
    }
}
