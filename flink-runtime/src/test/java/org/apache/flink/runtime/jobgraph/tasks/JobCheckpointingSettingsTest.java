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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JobCheckpointingSettingsTest {

    /** Tests that the settings are actually serializable. */
    @Test
    public void testIsJavaSerializable() throws Exception {
        JobCheckpointingSettings settings =
                new JobCheckpointingSettings(
                        new CheckpointCoordinatorConfiguration(
                                1231231,
                                1231,
                                112,
                                12,
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE,
                                false,
                                false,
                                false,
                                0),
                        new SerializedValue<>(new MemoryStateBackend()));

        JobCheckpointingSettings copy = CommonTestUtils.createCopySerializable(settings);
        assertEquals(
                settings.getCheckpointCoordinatorConfiguration(),
                copy.getCheckpointCoordinatorConfiguration());
        assertNotNull(copy.getDefaultStateBackend());
        assertTrue(
                copy.getDefaultStateBackend()
                                .deserializeValue(this.getClass().getClassLoader())
                                .getClass()
                        == MemoryStateBackend.class);
    }
}
