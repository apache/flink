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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link YarnWorkerResourceSpecFactory}. */
class YarnWorkerResourceSpecFactoryTest {

    @Test
    void testGetCpuCoresCommonOption() {
        final Configuration configuration = new Configuration();
        configuration.setDouble(TaskManagerOptions.CPU_CORES, 1.0);
        configuration.setInteger(YarnConfigOptions.VCORES, 2);
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        assertThat(YarnWorkerResourceSpecFactory.getDefaultCpus(configuration))
                .isEqualTo(new CPUResource(1.0));
    }

    @Test
    void testGetCpuCoresYarnOption() {
        final Configuration configuration = new Configuration();
        configuration.setInteger(YarnConfigOptions.VCORES, 2);
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);

        assertThat(YarnWorkerResourceSpecFactory.getDefaultCpus(configuration))
                .isEqualTo(new CPUResource(2.0));
    }

    @Test
    void testGetCpuCoresNumSlots() {
        final Configuration configuration = new Configuration();
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);

        assertThat(YarnWorkerResourceSpecFactory.getDefaultCpus(configuration))
                .isEqualTo(new CPUResource(3.0));
    }

    @Test
    void testGetCpuRoundUp() {
        final Configuration configuration = new Configuration();
        configuration.setDouble(TaskManagerOptions.CPU_CORES, 0.5);

        assertThat(YarnWorkerResourceSpecFactory.getDefaultCpus(configuration))
                .isEqualTo(new CPUResource(1.0));
    }

    @Test
    void testGetCpuExceedMaxInt() {
        final Configuration configuration = new Configuration();
        configuration.setDouble(TaskManagerOptions.CPU_CORES, Double.MAX_VALUE);
        assertThatThrownBy(() -> YarnWorkerResourceSpecFactory.getDefaultCpus(configuration))
                .isInstanceOf(IllegalConfigurationException.class);
    }
}
