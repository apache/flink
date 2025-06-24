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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MemorySize#toString()}. */
@ExtendWith(ParameterizedTestExtension.class)
class MemorySizePrettyPrintingTest {

    @Parameters(name = "memorySize = {0}, expectedString = {1}")
    private static Object[][] parameters() {
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

    @Parameter private MemorySize memorySize;

    @Parameter(value = 1)
    private String expectedString;

    @TestTemplate
    void testFormatting() {
        assertThat(memorySize.toString()).isEqualTo(expectedString);
    }
}
