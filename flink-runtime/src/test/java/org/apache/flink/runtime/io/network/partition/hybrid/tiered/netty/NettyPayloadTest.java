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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NettyPayload}. */
class NettyPayloadTest {

    @Test
    void testGetBuffer() {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        NettyPayload nettyPayload = NettyPayload.newBuffer(buffer, 0, 0);
        assertThat(nettyPayload.getBuffer()).isPresent();
        assertThat(nettyPayload.getBuffer()).hasValue(buffer);
        assertThatThrownBy(() -> NettyPayload.newBuffer(null, 0, 0))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> NettyPayload.newBuffer(buffer, -1, 0))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> NettyPayload.newBuffer(buffer, 0, -1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testGetError() {
        Throwable error = new RuntimeException("test exception");
        NettyPayload nettyPayload = NettyPayload.newError(error);
        assertThat(nettyPayload.getError()).isPresent();
        assertThat(nettyPayload.getError()).hasValue(error);
        assertThatThrownBy(() -> NettyPayload.newError(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testGetBufferIndex() {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        int bufferIndex = 0;
        int subpartitionId = 1;
        NettyPayload nettyPayload = NettyPayload.newBuffer(buffer, bufferIndex, subpartitionId);
        assertThat(nettyPayload.getBufferIndex()).isEqualTo(bufferIndex);
    }

    @Test
    void testGetSubpartitionId() {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        int bufferIndex = 0;
        int subpartitionId = 1;
        NettyPayload nettyPayload = NettyPayload.newBuffer(buffer, bufferIndex, subpartitionId);
        assertThat(nettyPayload.getSubpartitionId()).isEqualTo(subpartitionId);
    }

    @Test
    void testGetSegmentId() {
        int segmentId = 1;
        NettyPayload nettyPayload = NettyPayload.newSegment(segmentId);
        assertThat(nettyPayload.getSegmentId()).isEqualTo(segmentId);
        assertThatThrownBy(() -> NettyPayload.newSegment(-1))
                .isInstanceOf(IllegalStateException.class);
    }
}
