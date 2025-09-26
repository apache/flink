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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ApplicationOptionsInternal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/** Utility class to handle application/job related configuration options in application mode. */
public class ApplicationJobUtils {

    public static void maybeFixJobIdAndApplicationId(Configuration configuration) {
        final Optional<String> configuredJobId =
                configuration.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
        final Optional<String> configuredApplicationId =
                configuration.getOptional(ApplicationOptionsInternal.FIXED_APPLICATION_ID);

        if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
            if (!configuredJobId.isPresent()) {
                // In HA mode, we should use PIPELINE_FIXED_JOB_ID as static job id. Here, we
                // manually generate the job id, if not configured, from the cluster id to keep it
                // consistent across failover.
                configuration.set(
                        PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID,
                        new JobID(
                                        Preconditions.checkNotNull(
                                                        configuration.get(
                                                                HighAvailabilityOptions
                                                                        .HA_CLUSTER_ID))
                                                .hashCode(),
                                        0)
                                .toHexString());
            }
            if (!configuredApplicationId.isPresent()) {
                configuration.set(
                        ApplicationOptionsInternal.FIXED_APPLICATION_ID,
                        new ApplicationID(
                                        Preconditions.checkNotNull(
                                                        configuration.get(
                                                                HighAvailabilityOptions
                                                                        .HA_CLUSTER_ID))
                                                .hashCode(),
                                        0)
                                .toHexString());
            }
        }
    }

    public static boolean allowExecuteMultipleJobs(Configuration config) {
        final Optional<String> configuredJobId =
                config.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
        return !HighAvailabilityMode.isHighAvailabilityModeActivated(config)
                && !configuredJobId.isPresent();
    }
}
