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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageUtils}. */
public class TieredStorageUtilsTest {

    @Test
    void testGenerateBufferWithHeaders() {
        int bufferBytes = 5;
        Buffer originalBuffer = BufferBuilderTestUtils.buildSomeBuffer(bufferBytes);
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(originalBuffer, header);

        ByteBuffer[] byteBuffers =
                TieredStorageUtils.generateBufferWithHeaders(
                        Collections.singletonList(
                                new Tuple2<>(
                                        BufferBuilderTestUtils.buildSomeBuffer(bufferBytes), 0)));
        assertThat(byteBuffers).hasSize(2);
        assertThat(byteBuffers[0]).isEqualTo(header);
        assertThat(byteBuffers[1]).isEqualTo(originalBuffer.getNioBufferReadable());
    }
}
