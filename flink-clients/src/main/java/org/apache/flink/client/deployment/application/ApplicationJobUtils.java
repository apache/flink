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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/** Utility class to handle application/job related configuration options in application mode. */
public class ApplicationJobUtils {

    /**
     * Ensures deterministic application and job IDs in high availability (HA) mode.
     *
     * <p>In HA mode, fixed IDs are required to maintain state consistency across JobManager
     * failovers. This method guarantees that the application ID and job ID are properly set as
     * follows:
     *
     * <ul>
     *   <li>If no application ID is configured, it generates a fixed one from the HA cluster ID.
     *   <li>If no job ID is configured, it generates a fixed one based on the application ID (or
     *       the HA cluster ID if the application ID is also absent).
     * </ul>
     *
     * <p>If HA mode is disabled, this method does nothing; and the system will assign random
     * application/job IDs if none is configured.
     *
     * @param configuration The configuration the may be updated with fixed IDs
     */
    public static void maybeFixIds(Configuration configuration) {
        if (configuration.getOptional(ClusterOptions.CLUSTER_ID).isEmpty()
                && configuration.getOptional(HighAvailabilityOptions.HA_CLUSTER_ID).isPresent()) {
            // The CLUSTER_ID will fall back to the HA_CLUSTER_ID. If the user has already
            // configured HA_CLUSTER_ID, it can avoid conflicts caused by the missing CLUSTER_ID.
            configuration.set(
                    ClusterOptions.CLUSTER_ID,
                    new AbstractID(
                                    configuration
                                            .get(HighAvailabilityOptions.HA_CLUSTER_ID)
                                            .hashCode(),
                                    0)
                            .toHexString());
        }
        checkClusterId(configuration.get(ClusterOptions.CLUSTER_ID));

        if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
            final Optional<String> configuredClusterId =
                    configuration.getOptional(ClusterOptions.CLUSTER_ID);
            final Optional<String> configuredApplicationId =
                    configuration.getOptional(ApplicationOptionsInternal.FIXED_APPLICATION_ID);
            if (configuredApplicationId.isEmpty()) {
                // In HA mode, a fixed application id is required to ensure consistency across
                // failovers. The application id is derived from the cluster id.
                configuration.set(
                        ApplicationOptionsInternal.FIXED_APPLICATION_ID,
                        configuration.get(ClusterOptions.CLUSTER_ID));
            }
            final Optional<String> configuredJobId =
                    configuration.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
            if (configuredJobId.isEmpty()) {
                // In HA mode, a fixed job id is required to ensure consistency across failovers.
                // The job id is derived as follows:
                // 1. If either cluster id or application id is configured, use the application id
                // as the job id.
                // 2. Otherwise, generate the job id based on the HA cluster id.
                // Note that the second case is kept for backward compatibility and may be removed.
                if (configuredClusterId.isPresent() || configuredApplicationId.isPresent()) {
                    ApplicationID applicationId =
                            ApplicationID.fromHexString(
                                    configuration.get(
                                            ApplicationOptionsInternal.FIXED_APPLICATION_ID));
                    configuration.set(
                            PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID,
                            applicationId.toHexString());
                } else {
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
            }
        }
    }

    private static String checkClusterId(String str) {
        try {
            ApplicationID.fromHexString(str);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid cluster id \""
                            + str
                            + "\". The expected format is [0-9a-fA-F]{32}, e.g. fd72014d4c864993a2e5a9287b4a9c5d.");
        }
        return str;
    }

    public static boolean allowExecuteMultipleJobs(Configuration config) {
        final Optional<String> configuredJobId =
                config.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);
        return !HighAvailabilityMode.isHighAvailabilityModeActivated(config)
                && configuredJobId.isEmpty();
    }
}
