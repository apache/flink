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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskmanager.NetworkMemoryConfiguration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link org.apache.flink.runtime.taskmanager.NetworkMemoryConfiguration}. */
public class NetworkMemoryConfigurationTest {

    private static final MemorySize MEM_SIZE_PARAM = new MemorySize(128L * 1024 * 1024);

    @Test
    public void testNetworkBufferNumberCalculation() {
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("1m"));
        final int numNetworkBuffers =
                NetworkMemoryConfiguration.fromConfiguration(config, MEM_SIZE_PARAM)
                        .getNumNetworkBuffers();
        assertEquals(128, numNetworkBuffers);
    }
}
