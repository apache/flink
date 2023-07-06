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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Tests for {@link SubpartitionDiskCacheManager}. */
class SubpartitionDiskCacheManagerTest {

    @Test
    void testAppendBuffer() {
        int bufferBytes = 5;
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                new SubpartitionDiskCacheManager();
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(bufferBytes);
        subpartitionDiskCacheManager.append(buffer);
        List<Tuple2<Buffer, Integer>> bufferAndIndexes =
                subpartitionDiskCacheManager.removeAllBuffers();

        assertThat(bufferAndIndexes).hasSize(1);
        assertThat(bufferAndIndexes.get(0)).isNotNull();
        assertThat(bufferAndIndexes.get(0).f0.readableBytes()).isEqualTo(bufferBytes);
    }

    @Test
    void testAppendEndOfSegmentRecord() throws IOException {
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                new SubpartitionDiskCacheManager();
        subpartitionDiskCacheManager.appendEndOfSegmentEvent(
                EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE));
        List<Tuple2<Buffer, Integer>> bufferAndIndexes =
                subpartitionDiskCacheManager.removeAllBuffers();

        assertThat(bufferAndIndexes).hasSize(1);
        assertThat(bufferAndIndexes.get(0)).isNotNull();
        Buffer buffer = bufferAndIndexes.get(0).f0;
        assertThat(buffer.isBuffer()).isFalse();
        AbstractEvent event =
                EventSerializer.fromSerializedEvent(
                        buffer.readOnlySlice().getNioBufferReadable(), getClass().getClassLoader());
        assertThat(event).isInstanceOf(EndOfSegmentEvent.class);
    }

    @Test
    void testRelease() {
        SubpartitionDiskCacheManager subpartitionDiskCacheManager =
                new SubpartitionDiskCacheManager();
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer();
        subpartitionDiskCacheManager.append(buffer);
        List<Tuple2<Buffer, Integer>> bufferAndIndexes =
                subpartitionDiskCacheManager.removeAllBuffers();
        assertThat(bufferAndIndexes).hasSize(1);
        assertThat(bufferAndIndexes.get(0).f0).isEqualTo(buffer);
        assertThatNoException().isThrownBy(subpartitionDiskCacheManager::release);
    }
}
