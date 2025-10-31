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

package org.apache.flink.connector.file.src.compression;

import org.apache.flink.api.common.io.compression.Bzip2InputStreamFactory;
import org.apache.flink.api.common.io.compression.DeflateInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.GzipInflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.api.common.io.compression.XZInputStreamFactory;
import org.apache.flink.api.common.io.compression.ZStandardInputStreamFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StandardDeCompressorsTest {

    @Test
    void testDeflateDeCompressor() {
        assertThat(StandardDeCompressors.getCommonSuffixes()).contains("deflate");

        InflaterInputStreamFactory<?> inputStreamFactory =
                DeflateInflaterInputStreamFactory.getInstance();
        assertThat(StandardDeCompressors.getDecompressorForExtension("deflate"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForFileName("file.pcap.deflate"))
                .isEqualTo(inputStreamFactory);
    }

    @Test
    void testGZIPDeCompressor() {
        assertThat(StandardDeCompressors.getCommonSuffixes()).contains("gz").contains("gzip");

        InflaterInputStreamFactory<?> inputStreamFactory =
                GzipInflaterInputStreamFactory.getInstance();
        assertThat(StandardDeCompressors.getDecompressorForExtension("gz"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForExtension("gzip"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForFileName("file.pcap.gz"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForFileName("file.pcap.gzip"))
                .isEqualTo(inputStreamFactory);
    }

    @Test
    void testBzip2DeCompressor() {
        assertThat(StandardDeCompressors.getCommonSuffixes()).contains("bz2");

        InflaterInputStreamFactory<?> inputStreamFactory = Bzip2InputStreamFactory.getInstance();
        assertThat(StandardDeCompressors.getDecompressorForExtension("bz2"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForFileName("file.pcap.bz2"))
                .isEqualTo(inputStreamFactory);
    }

    @Test
    void testXZDeCompressor() {
        assertThat(StandardDeCompressors.getCommonSuffixes()).contains("xz");

        InflaterInputStreamFactory<?> inputStreamFactory = XZInputStreamFactory.getInstance();
        assertThat(StandardDeCompressors.getDecompressorForExtension("xz"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForFileName("file.pcap.xz"))
                .isEqualTo(inputStreamFactory);
    }

    @Test
    void testZStandardDeCompressor() {
        assertThat(StandardDeCompressors.getCommonSuffixes()).contains("zst");

        InflaterInputStreamFactory<?> inputStreamFactory =
                ZStandardInputStreamFactory.getInstance();
        assertThat(StandardDeCompressors.getDecompressorForExtension("zst"))
                .isEqualTo(inputStreamFactory);
        assertThat(StandardDeCompressors.getDecompressorForFileName("file.pcap.zst"))
                .isEqualTo(inputStreamFactory);
    }
}
