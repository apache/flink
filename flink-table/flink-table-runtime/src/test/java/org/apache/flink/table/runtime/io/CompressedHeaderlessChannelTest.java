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

package org.apache.flink.table.runtime.io;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CompressedHeaderlessChannelReaderInputView} and {@link
 * CompressedHeaderlessChannelWriterOutputView}.
 */
class CompressedHeaderlessChannelTest {
    private static final int BUFFER_SIZE = 256;

    private IOManager ioManager;

    static Collection<Arguments> compressionFactory() {
        return Arrays.asList(
                Arguments.of(
                        CompressionCodec.LZ4,
                        BlockCompressionFactory.createBlockCompressionFactory(
                                CompressionCodec.LZ4)),
                Arguments.of(
                        CompressionCodec.LZO,
                        BlockCompressionFactory.createBlockCompressionFactory(
                                CompressionCodec.LZO)),
                Arguments.of(
                        CompressionCodec.ZSTD,
                        BlockCompressionFactory.createBlockCompressionFactory(
                                CompressionCodec.ZSTD)));
    }

    CompressedHeaderlessChannelTest() {
        ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        this.ioManager.close();
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("compressionFactory")
    void testCompressedView(
            final CompressionCodec codec, final BlockCompressionFactory compressionFactory)
            throws IOException {
        for (int testTime = 0; testTime < 10; testTime++) {
            int testRounds = new Random().nextInt(20000);
            FileIOChannel.ID channel = ioManager.createChannel();
            BufferFileWriter writer = this.ioManager.createBufferFileWriter(channel);
            CompressedHeaderlessChannelWriterOutputView outputView =
                    new CompressedHeaderlessChannelWriterOutputView(
                            writer, compressionFactory, BUFFER_SIZE);

            for (int i = 0; i < testRounds; i++) {
                outputView.writeInt(i);
            }
            outputView.close();
            int blockCount = outputView.getBlockCount();

            CompressedHeaderlessChannelReaderInputView inputView =
                    new CompressedHeaderlessChannelReaderInputView(
                            channel, ioManager, compressionFactory, BUFFER_SIZE, blockCount);

            for (int i = 0; i < testRounds; i++) {
                assertThat(inputView.readInt()).isEqualTo(i);
            }
            inputView.close();
        }
    }
}
