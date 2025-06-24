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

package org.apache.flink.runtime.io.network.partition.hybrid.index;

import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.partition.hybrid.index.FileRegionWriteReadUtils.allocateAndConfigureBuffer;

/** Testing implementation for {@link FileDataIndexRegionHelper.Region}. */
public class TestingFileDataIndexRegion implements FileDataIndexRegionHelper.Region {

    public static final int REGION_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

    private final Supplier<Integer> getSizeSupplier;

    private final Supplier<Integer> getFirstBufferIndexSupplier;

    private final Supplier<Long> getRegionFileOffsetSupplier;

    private final Supplier<Long> getRegionFileEndOffsetSupplier;

    private final Supplier<Integer> getNumBuffersSupplier;

    private final Function<Integer, Boolean> containBufferFunction;

    private TestingFileDataIndexRegion(
            Supplier<Integer> getSizeSupplier,
            Supplier<Integer> getFirstBufferIndexSupplier,
            Supplier<Long> getRegionFileOffsetSupplier,
            Supplier<Long> getRegionFileEndOffsetSupplier,
            Supplier<Integer> getNumBuffersSupplier,
            Function<Integer, Boolean> containBufferFunction) {
        this.getSizeSupplier = getSizeSupplier;
        this.getFirstBufferIndexSupplier = getFirstBufferIndexSupplier;
        this.getRegionFileOffsetSupplier = getRegionFileOffsetSupplier;
        this.getRegionFileEndOffsetSupplier = getRegionFileEndOffsetSupplier;
        this.getNumBuffersSupplier = getNumBuffersSupplier;
        this.containBufferFunction = containBufferFunction;
    }

    @Override
    public int getSize() {
        return getSizeSupplier.get();
    }

    @Override
    public int getFirstBufferIndex() {
        return getFirstBufferIndexSupplier.get();
    }

    @Override
    public long getRegionStartOffset() {
        return getRegionFileOffsetSupplier.get();
    }

    @Override
    public long getRegionEndOffset() {
        return getRegionFileEndOffsetSupplier.get();
    }

    @Override
    public int getNumBuffers() {
        return getNumBuffersSupplier.get();
    }

    @Override
    public boolean containBuffer(int bufferIndex) {
        return containBufferFunction.apply(bufferIndex);
    }

    public static void writeRegionToFile(FileChannel channel, TestingFileDataIndexRegion region)
            throws IOException {
        ByteBuffer regionBuffer = allocateHeaderBuffer();
        regionBuffer.clear();
        regionBuffer.putInt(region.getFirstBufferIndex());
        regionBuffer.putInt(region.getNumBuffers());
        regionBuffer.putLong(region.getRegionStartOffset());
        regionBuffer.flip();
        BufferReaderWriterUtil.writeBuffers(channel, regionBuffer.capacity(), regionBuffer);
    }

    public static TestingFileDataIndexRegion readRegionFromFile(
            FileChannel channel, long fileOffset) throws IOException {
        ByteBuffer regionBuffer = allocateHeaderBuffer();
        regionBuffer.clear();
        BufferReaderWriterUtil.readByteBufferFully(channel, regionBuffer, fileOffset);
        regionBuffer.flip();
        int firstBufferIndex = regionBuffer.getInt();
        int numBuffers = regionBuffer.getInt();
        long firstBufferOffset = regionBuffer.getLong();
        return new TestingFileDataIndexRegion.Builder()
                .setGetSizeSupplier(() -> TestingFileDataIndexRegion.REGION_SIZE)
                .setGetFirstBufferIndexSupplier(() -> firstBufferIndex)
                .setGetRegionFileOffsetSupplier(() -> firstBufferOffset)
                .setGetNumBuffersSupplier(() -> numBuffers)
                .setContainBufferFunction(
                        bufferIndex ->
                                getContainBufferFunction(bufferIndex, firstBufferIndex, numBuffers))
                .build();
    }

    private static ByteBuffer allocateHeaderBuffer() {
        return allocateAndConfigureBuffer(TestingFileDataIndexRegion.REGION_SIZE);
    }

    public static boolean getContainBufferFunction(
            int bufferIndex, int firstBufferIndex, int numBuffers) {
        return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
    }

    /** Builder for {@link TestingFileDataIndexRegion}. */
    public static class Builder {

        private Supplier<Integer> getSizeSupplier = () -> 0;

        private Supplier<Integer> getFirstBufferIndexSupplier = () -> 0;

        private Supplier<Long> getRegionFileOffsetSupplier = () -> 0L;

        private Supplier<Long> getRegionFileEndOffsetSupplier = () -> 0L;

        private Supplier<Integer> getNumBuffersSupplier = () -> 0;

        private Function<Integer, Boolean> containBufferFunction = bufferIndex -> false;

        public TestingFileDataIndexRegion.Builder setGetSizeSupplier(
                Supplier<Integer> getSizeSupplier) {
            this.getSizeSupplier = getSizeSupplier;
            return this;
        }

        public TestingFileDataIndexRegion.Builder setGetFirstBufferIndexSupplier(
                Supplier<Integer> getFirstBufferIndexSupplier) {
            this.getFirstBufferIndexSupplier = getFirstBufferIndexSupplier;
            return this;
        }

        public TestingFileDataIndexRegion.Builder setGetRegionFileOffsetSupplier(
                Supplier<Long> getRegionFileOffsetSupplier) {
            this.getRegionFileOffsetSupplier = getRegionFileOffsetSupplier;
            return this;
        }

        public TestingFileDataIndexRegion.Builder setGetRegionFileEndOffsetSupplier(
                Supplier<Long> getRegionFileEndOffsetSupplier) {
            this.getRegionFileEndOffsetSupplier = getRegionFileEndOffsetSupplier;
            return this;
        }

        public TestingFileDataIndexRegion.Builder setGetNumBuffersSupplier(
                Supplier<Integer> getNumBuffersSupplier) {
            this.getNumBuffersSupplier = getNumBuffersSupplier;
            return this;
        }

        public TestingFileDataIndexRegion.Builder setContainBufferFunction(
                Function<Integer, Boolean> containBufferFunction) {
            this.containBufferFunction = containBufferFunction;
            return this;
        }

        public TestingFileDataIndexRegion build() {
            return new TestingFileDataIndexRegion(
                    getSizeSupplier,
                    getFirstBufferIndexSupplier,
                    getRegionFileOffsetSupplier,
                    getRegionFileEndOffsetSupplier,
                    getNumBuffersSupplier,
                    containBufferFunction);
        }
    }
}
