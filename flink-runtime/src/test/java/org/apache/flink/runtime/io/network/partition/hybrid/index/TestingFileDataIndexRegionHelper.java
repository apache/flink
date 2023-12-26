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

import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;

import java.io.IOException;
import java.nio.channels.FileChannel;

/** Testing implementation for {@link FileDataIndexRegionHelper}. */
public class TestingFileDataIndexRegionHelper
        implements FileDataIndexRegionHelper<TestingFileDataIndexRegion> {

    private final BiConsumerWithException<FileChannel, TestingFileDataIndexRegion, IOException>
            writeRegionToFileConsumer;

    private final BiFunctionWithException<
                    FileChannel, Long, TestingFileDataIndexRegion, IOException>
            readRegionFromFileFunction;

    private TestingFileDataIndexRegionHelper(
            BiConsumerWithException<FileChannel, TestingFileDataIndexRegion, IOException>
                    writeRegionToFileConsumer,
            BiFunctionWithException<FileChannel, Long, TestingFileDataIndexRegion, IOException>
                    readRegionFromFileFunction) {
        this.writeRegionToFileConsumer = writeRegionToFileConsumer;
        this.readRegionFromFileFunction = readRegionFromFileFunction;
    }

    @Override
    public void writeRegionToFile(FileChannel channel, TestingFileDataIndexRegion region)
            throws IOException {
        writeRegionToFileConsumer.accept(channel, region);
    }

    @Override
    public TestingFileDataIndexRegion readRegionFromFile(FileChannel channel, long fileOffset)
            throws IOException {
        return readRegionFromFileFunction.apply(channel, fileOffset);
    }

    /** Builder for {@link TestingFileDataIndexRegionHelper}. */
    public static class Builder {
        private BiConsumerWithException<FileChannel, TestingFileDataIndexRegion, IOException>
                writeRegionToFileConsumer = (fileChannel, testRegion) -> {};

        private BiFunctionWithException<FileChannel, Long, TestingFileDataIndexRegion, IOException>
                readRegionFromFileFunction = (fileChannel, fileOffset) -> null;

        public TestingFileDataIndexRegionHelper.Builder setWriteRegionToFileConsumer(
                BiConsumerWithException<FileChannel, TestingFileDataIndexRegion, IOException>
                        writeRegionToFileConsumer) {
            this.writeRegionToFileConsumer = writeRegionToFileConsumer;
            return this;
        }

        public TestingFileDataIndexRegionHelper.Builder setReadRegionFromFileFunction(
                BiFunctionWithException<FileChannel, Long, TestingFileDataIndexRegion, IOException>
                        readRegionFromFileFunction) {
            this.readRegionFromFileFunction = readRegionFromFileFunction;
            return this;
        }

        public TestingFileDataIndexRegionHelper build() {
            return new TestingFileDataIndexRegionHelper(
                    writeRegionToFileConsumer, readRegionFromFileFunction);
        }
    }
}
