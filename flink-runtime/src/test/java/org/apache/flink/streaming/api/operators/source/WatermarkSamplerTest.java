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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link WatermarkSampler}. */
public class WatermarkSamplerTest {

    @Test
    public void testCapacity0() {
        WatermarkSampler ringBuffer = new WatermarkSampler(0);

        assertThat(ringBuffer.getOldestSample()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());
        assertThat(ringBuffer.getLatest()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());

        ringBuffer.addLatest(13L);
        assertThat(ringBuffer.getOldestSample()).isEqualTo(13L);
        assertThat(ringBuffer.getLatest()).isEqualTo(13L);
        ringBuffer.addLatest(42L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(42L);
        assertThat(ringBuffer.getLatest()).isEqualTo(42L);
    }

    @Test
    public void testCapacity2() {
        WatermarkSampler ringBuffer = new WatermarkSampler(2);

        assertThat(ringBuffer.getOldestSample()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());
        assertThat(ringBuffer.getLatest()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());

        ringBuffer.addLatest(13L);
        ringBuffer.addLatest(42L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());
        assertThat(ringBuffer.getLatest()).isEqualTo(42L);

        ringBuffer.addLatest(44L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(42L);
        assertThat(ringBuffer.getLatest()).isEqualTo(44L);

        ringBuffer.addLatest(1337L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(44L);
        assertThat(ringBuffer.getLatest()).isEqualTo(1337L);
    }

    @Test
    public void testCapacity3() {
        WatermarkSampler ringBuffer = new WatermarkSampler(3);

        assertThat(ringBuffer.getOldestSample()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());
        assertThat(ringBuffer.getLatest()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());

        ringBuffer.addLatest(42L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());
        assertThat(ringBuffer.getLatest()).isEqualTo(42L);

        ringBuffer.addLatest(44L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(Watermark.UNINITIALIZED.getTimestamp());
        assertThat(ringBuffer.getLatest()).isEqualTo(44L);

        ringBuffer.addLatest(1337L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(42L);
        assertThat(ringBuffer.getLatest()).isEqualTo(1337L);

        ringBuffer.addLatest(0L);
        ringBuffer.sample();
        assertThat(ringBuffer.getOldestSample()).isEqualTo(44L);
        assertThat(ringBuffer.getLatest()).isEqualTo(0L);
    }
}
