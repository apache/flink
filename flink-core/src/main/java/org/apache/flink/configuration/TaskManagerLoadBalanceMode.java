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

import org.apache.flink.annotation.Internal;

import javax.annotation.Nonnull;

/** Type of {@link TaskManagerOptions#TASK_MANAGER_LOAD_BALANCE_MODE}. */
@Internal
public enum TaskManagerLoadBalanceMode {
    NONE,
    SLOTS;

    /**
     * The method is mainly to load the {@link TaskManagerOptions#TASK_MANAGER_LOAD_BALANCE_MODE}
     * from {@link Configuration}, which is compatible with {@link
     * ClusterOptions#EVENLY_SPREAD_OUT_SLOTS_STRATEGY}.
     */
    public static TaskManagerLoadBalanceMode loadFromConfiguration(
            @Nonnull Configuration configuration) {
        TaskManagerLoadBalanceMode taskManagerLoadBalanceMode =
                configuration.get(TaskManagerOptions.TASK_MANAGER_LOAD_BALANCE_MODE);
        boolean evenlySpreadOutSlots =
                configuration.getBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY);

        if (taskManagerLoadBalanceMode != TaskManagerLoadBalanceMode.NONE
                && taskManagerLoadBalanceMode != null) {
            return taskManagerLoadBalanceMode;
        }
        if (evenlySpreadOutSlots) {
            return TaskManagerLoadBalanceMode.SLOTS;
        }
        return taskManagerLoadBalanceMode == null
                ? TaskManagerLoadBalanceMode.NONE
                : taskManagerLoadBalanceMode;
    }
}
