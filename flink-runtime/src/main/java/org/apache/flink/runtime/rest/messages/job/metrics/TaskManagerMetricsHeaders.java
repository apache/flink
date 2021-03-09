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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;

/**
 * {@link MessageHeaders} for {@link
 * org.apache.flink.runtime.rest.handler.job.metrics.TaskManagerMetricsHandler}.
 */
public final class TaskManagerMetricsHeaders
        extends AbstractMetricsHeaders<TaskManagerMetricsMessageParameters> {

    private static final TaskManagerMetricsHeaders INSTANCE = new TaskManagerMetricsHeaders();

    private TaskManagerMetricsHeaders() {}

    @Override
    public TaskManagerMetricsMessageParameters getUnresolvedMessageParameters() {
        return new TaskManagerMetricsMessageParameters();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/taskmanagers/:" + TaskManagerIdPathParameter.KEY + "/metrics";
    }

    public static TaskManagerMetricsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Provides access to task manager metrics.";
    }
}
