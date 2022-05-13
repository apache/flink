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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpecFactory;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link WorkerResourceSpecFactory} for Yarn deployments. */
public class YarnWorkerResourceSpecFactory extends WorkerResourceSpecFactory {

    public static final YarnWorkerResourceSpecFactory INSTANCE =
            new YarnWorkerResourceSpecFactory();

    private static final Logger LOG = LoggerFactory.getLogger(YarnWorkerResourceSpecFactory.class);

    private YarnWorkerResourceSpecFactory() {}

    @Override
    public WorkerResourceSpec createDefaultWorkerResourceSpec(Configuration configuration) {
        return workerResourceSpecFromConfigAndCpu(configuration, getDefaultCpus(configuration));
    }

    @VisibleForTesting
    static CPUResource getDefaultCpus(final Configuration configuration) {
        int fallback = configuration.getInteger(YarnConfigOptions.VCORES);
        double cpuCoresDouble =
                TaskExecutorProcessUtils.getCpuCoresWithFallback(configuration, fallback)
                        .getValue()
                        .doubleValue();
        @SuppressWarnings("NumericCastThatLosesPrecision")
        long cpuCoresLong = Math.max((long) Math.ceil(cpuCoresDouble), 1L);
        //noinspection FloatingPointEquality
        if (cpuCoresLong != cpuCoresDouble) {
            LOG.info(
                    "The amount of cpu cores must be a positive integer on Yarn. Rounding {} up to the closest positive integer {}.",
                    cpuCoresDouble,
                    cpuCoresLong);
        }
        if (cpuCoresLong > Integer.MAX_VALUE) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The amount of cpu cores %d cannot exceed Integer.MAX_VALUE: %d",
                            cpuCoresLong, Integer.MAX_VALUE));
        }
        //noinspection NumericCastThatLosesPrecision
        return new CPUResource(cpuCoresLong);
    }
}
