/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * Context information provided to plugins at initialization time, including the ID of the task
 * manager that hosts the plugin.
 */
@PublicEvolving
public class PluginContext {
    private final String taskManagerId;

    public PluginContext(String taskManagerId) {
        this.taskManagerId =
                Preconditions.checkNotNull(taskManagerId, "taskManagerId must not be null");
    }

    public String getTaskManagerId() {
        return taskManagerId;
    }

    @Override
    public String toString() {
        return "PluginContext{taskManagerId='" + taskManagerId + "'}";
    }
}
