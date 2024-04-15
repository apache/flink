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

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_LOAD_BALANCE_MODE;
import static org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TaskManagerLoadBalanceMode}. */
class TaskManagerLoadBalanceModeTest {

    @Test
    void testReadTaskManagerLoadBalanceMode() {
        // Check for non-set 'taskmanager.load-balance.mode' and
        // 'cluster.evenly-spread-out-slots: false'
        Configuration conf1 = new Configuration();
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf1))
                .isEqualTo(TASK_MANAGER_LOAD_BALANCE_MODE.defaultValue());

        // Check for non-set 'taskmanager.load-balance.mode' and
        // 'cluster.evenly-spread-out-slots: true'
        Configuration conf2 = new Configuration();
        conf2.set(EVENLY_SPREAD_OUT_SLOTS_STRATEGY, true);
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf2))
                .isEqualTo(TaskManagerLoadBalanceMode.SLOTS);

        // Check for setting manually 'taskmanager.load-balance.mode: NONE' and
        // 'cluster.evenly-spread-out-slots: false'
        Configuration conf3 = new Configuration();
        conf3.set(TASK_MANAGER_LOAD_BALANCE_MODE, TaskManagerLoadBalanceMode.NONE);
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf3))
                .isEqualTo(TaskManagerLoadBalanceMode.NONE);

        // Check for setting manually 'taskmanager.load-balance.mode: NONE' and
        // 'cluster.evenly-spread-out-slots: true'
        Configuration conf4 = new Configuration();
        conf4.set(TASK_MANAGER_LOAD_BALANCE_MODE, TaskManagerLoadBalanceMode.NONE);
        conf4.set(EVENLY_SPREAD_OUT_SLOTS_STRATEGY, true);
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf4))
                .isEqualTo(TaskManagerLoadBalanceMode.NONE);

        // Check for setting manually 'taskmanager.load-balance.mode: SLOTS' and
        // 'cluster.evenly-spread-out-slots: false'
        Configuration conf5 = new Configuration();
        conf5.set(TASK_MANAGER_LOAD_BALANCE_MODE, TaskManagerLoadBalanceMode.SLOTS);
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf5))
                .isEqualTo(TaskManagerLoadBalanceMode.SLOTS);

        // Check for setting manually 'taskmanager.load-balance.mode: SLOTS' and
        // 'cluster.evenly-spread-out-slots: true'
        Configuration conf6 = new Configuration();
        conf6.set(TASK_MANAGER_LOAD_BALANCE_MODE, TaskManagerLoadBalanceMode.SLOTS);
        conf6.set(EVENLY_SPREAD_OUT_SLOTS_STRATEGY, true);
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf6))
                .isEqualTo(TaskManagerLoadBalanceMode.SLOTS);
    }
}
