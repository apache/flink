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

package org.apache.flink.configuration;

import org.apache.flink.configuration.MemorySize.MemoryUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MemorySize#toString()}. */
public class MemorySizePrettyPrintingTest {
    public static Object[][] parameters() {
        return new Object[][] {
            new Object[] {new MemorySize(MemoryUnit.KILO_BYTES.getMultiplier() + 1), "1025 bytes"},
            new Object[] {new MemorySize(100), "100 bytes"},
            new Object[] {new MemorySize(1024), "1 kb"},
            new Object[] {
                new MemorySize(MemoryUnit.GIGA_BYTES.getMultiplier() + 1),
                String.format("%d %s", MemoryUnit.GIGA_BYTES.getMultiplier() + 1, "bytes")
            },
            new Object[] {new MemorySize(0), "0 bytes"}
        };
    }

    public MemorySize memorySize;
    public String expectedString;

    @MethodSource("parameters")
    @ParameterizedTest
    void testFormatting(MemorySize memorySize, String expectedString) {
        initMemorySizePrettyPrintingTest(memorySize, expectedString);
        assertThat(memorySize.toString()).isEqualTo(expectedString);
    }

    public void initMemorySizePrettyPrintingTest(MemorySize memorySize, String expectedString) {
        this.memorySize = memorySize;
        this.expectedString = expectedString;
    }
}
