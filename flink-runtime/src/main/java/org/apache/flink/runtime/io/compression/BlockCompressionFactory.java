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

package org.apache.flink.runtime.io.compression;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;

import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Each compression codec has a implementation of {@link BlockCompressionFactory} to create
 * compressors and decompressors.
 */
public interface BlockCompressionFactory {

    BlockCompressor getCompressor();

    BlockDecompressor getDecompressor();

    /**
     * Creates {@link BlockCompressionFactory} according to the configuration.
     *
     * @param compressionName supported compression codecs.
     */
    static BlockCompressionFactory createBlockCompressionFactory(CompressionCodec compressionName) {

        checkNotNull(compressionName);

        BlockCompressionFactory blockCompressionFactory;
        switch (compressionName) {
            case LZ4:
                blockCompressionFactory = new Lz4BlockCompressionFactory();
                break;
            case LZO:
                blockCompressionFactory =
                        new AirCompressorFactory(new LzoCompressor(), new LzoDecompressor());
                break;
            case ZSTD:
                blockCompressionFactory =
                        new AirCompressorFactory(new ZstdCompressor(), new ZstdDecompressor());
                break;
            default:
                throw new IllegalStateException("Unknown CompressionMethod " + compressionName);
        }

        return blockCompressionFactory;
    }
}
