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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

/** Configuration class for the {@link ResourceManagerRuntimeServices} class. */
public class ResourceManagerRuntimeServicesConfiguration {

    private final Time jobTimeout;

    private final SlotManagerConfiguration slotManagerConfiguration;

    private final boolean enableFineGrainedResourceManagement;

    public ResourceManagerRuntimeServicesConfiguration(
            Time jobTimeout,
            SlotManagerConfiguration slotManagerConfiguration,
            boolean enableFineGrainedResourceManagement) {
        this.jobTimeout = Preconditions.checkNotNull(jobTimeout);
        this.slotManagerConfiguration = Preconditions.checkNotNull(slotManagerConfiguration);
        this.enableFineGrainedResourceManagement = enableFineGrainedResourceManagement;
    }

    public Time getJobTimeout() {
        return jobTimeout;
    }

    public SlotManagerConfiguration getSlotManagerConfiguration() {
        return slotManagerConfiguration;
    }

    public boolean isEnableFineGrainedResourceManagement() {
        return enableFineGrainedResourceManagement;
    }

    // ---------------------------- Static methods ----------------------------------

    public static ResourceManagerRuntimeServicesConfiguration fromConfiguration(
            Configuration configuration, WorkerResourceSpecFactory defaultWorkerResourceSpecFactory)
            throws ConfigurationException {

        final String strJobTimeout = configuration.getString(ResourceManagerOptions.JOB_TIMEOUT);
        final Time jobTimeout;

        try {
            jobTimeout = Time.milliseconds(TimeUtils.parseDuration(strJobTimeout).toMillis());
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(
                    "Could not parse the resource manager's job timeout "
                            + "value "
                            + ResourceManagerOptions.JOB_TIMEOUT
                            + '.',
                    e);
        }

        final WorkerResourceSpec defaultWorkerResourceSpec =
                defaultWorkerResourceSpecFactory.createDefaultWorkerResourceSpec(configuration);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration, defaultWorkerResourceSpec);

        final boolean enableFineGrainedResourceManagement =
                configuration.getBoolean(ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT);

        return new ResourceManagerRuntimeServicesConfiguration(
                jobTimeout, slotManagerConfiguration, enableFineGrainedResourceManagement);
    }
}
