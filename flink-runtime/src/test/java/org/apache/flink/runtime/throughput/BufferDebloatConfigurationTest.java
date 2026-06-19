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

package org.apache.flink.runtime.throughput;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for validation of {@link BufferDebloatConfiguration}. */
class BufferDebloatConfigurationTest extends TestLogger {

    @Test
    public void testMinGreaterThanMaxBufferSize() {
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE, new MemorySize(50));
        config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, new MemorySize(49));
        assertThrows(
                IllegalArgumentException.class,
                () -> BufferDebloatConfiguration.fromConfiguration(config));
    }

    @Test
    public void testNegativeConsumptionTime() {
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.BUFFER_DEBLOAT_TARGET, Duration.ofMillis(-1));
        assertThrows(
                IllegalArgumentException.class,
                () -> BufferDebloatConfiguration.fromConfiguration(config));
    }
}
