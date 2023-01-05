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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion.FIXED_SIZE;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.assertRegionEquals;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createSingleUnreleasedRegion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InternalRegionWriteReadUtils}. */
class InternalRegionWriteReadUtilsTest {
    @Test
    void testAllocateAndConfigureBuffer() {
        final int bufferSize = 16;
        ByteBuffer buffer = InternalRegionWriteReadUtils.allocateAndConfigureBuffer(bufferSize);
        assertThat(buffer.capacity()).isEqualTo(16);
        assertThat(buffer.limit()).isEqualTo(16);
        assertThat(buffer.position()).isZero();
        assertThat(buffer.isDirect()).isTrue();
        assertThat(buffer.order()).isEqualTo(ByteOrder.nativeOrder());
    }

    @Test
    void testReadPrematureEndOfFile(@TempDir Path tmpPath) throws Exception {
        FileChannel channel = tmpFileChannel(tmpPath);
        ByteBuffer buffer = InternalRegionWriteReadUtils.allocateAndConfigureBuffer(FIXED_SIZE);
        InternalRegionWriteReadUtils.writeRegionToFile(
                channel, buffer, createSingleUnreleasedRegion(0, 0L, 1));
        channel.truncate(channel.position() - 1);
        buffer.flip();
        assertThatThrownBy(
                        () -> InternalRegionWriteReadUtils.readRegionFromFile(channel, buffer, 0L))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testWriteAndReadRegion(@TempDir Path tmpPath) throws Exception {
        FileChannel channel = tmpFileChannel(tmpPath);
        ByteBuffer buffer = InternalRegionWriteReadUtils.allocateAndConfigureBuffer(FIXED_SIZE);
        InternalRegion region = createSingleUnreleasedRegion(10, 100L, 1);
        InternalRegionWriteReadUtils.writeRegionToFile(channel, buffer, region);
        buffer.flip();
        InternalRegion readRegion =
                InternalRegionWriteReadUtils.readRegionFromFile(channel, buffer, 0L);
        assertRegionEquals(readRegion, region);
    }

    private static FileChannel tmpFileChannel(Path tempPath) throws IOException {
        return FileChannel.open(
                Files.createFile(tempPath.resolve(UUID.randomUUID().toString())),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }
}
