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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.Map;

/**
 * A factory to create a {@link WorkflowScheduler} instance.
 *
 * <p>See {@link Factory} for more information about the general design of a factory.
 */
@PublicEvolving
public interface WorkflowSchedulerFactory extends Factory {

    /** Create a workflow scheduler instance which interacts with external scheduler service. */
    WorkflowScheduler<?> createWorkflowScheduler(Context context);

    /** Context provided when a workflow scheduler is created. */
    @PublicEvolving
    interface Context {

        /** Gives the config option to create {@link WorkflowScheduler}. */
        ReadableConfig getConfiguration();

        /**
         * Returns the options with which the workflow scheduler is created. All options that are
         * prefixed with the workflow scheduler identifier are included in the map.
         *
         * <p>All the keys in the options are pruned with the prefix. For example, the option {@code
         * workflow-scheduler.airflow.endpoint}'s key is {@code endpoint} in the map.
         *
         * <p>An implementation should perform validation of these options.
         */
        Map<String, String> getWorkflowSchedulerOptions();
    }
}
