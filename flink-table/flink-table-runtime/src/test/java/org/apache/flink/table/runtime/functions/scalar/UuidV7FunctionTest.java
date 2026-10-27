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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.types.logical.UuidType;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UuidV7Function#generate()}. */
class UuidV7FunctionTest {

    @Test
    void testGenerateLength() {
        assertThat(UuidV7Function.generate()).hasSize(UuidType.BYTE_LENGTH);
    }

    @Test
    void testGenerateVersionAndVariant() {
        byte[] bytes = UuidV7Function.generate();
        assertThat(bytes[6] & 0xF0).as("version nibble").isEqualTo(0x70);
        assertThat(bytes[8] & 0xC0).as("variant bits").isEqualTo(0x80);
    }

    @Test
    void testGenerateTimestamp() {
        long now = System.currentTimeMillis();
        byte[] bytes = UuidV7Function.generate();
        long timestamp = extractTimestamp(bytes);

        // The tolerance absorbs scheduling gaps and small clock corrections.
        long tolerance = Duration.ofSeconds(10).toMillis();
        assertThat(timestamp).isBetween(now - tolerance, now + tolerance);
    }

    private static long extractTimestamp(byte[] bytes) {
        long timestamp = 0;
        for (int i = 0; i < 6; i++) {
            timestamp = (timestamp << 8) | (bytes[i] & 0xFFL);
        }
        return timestamp;
    }
}
