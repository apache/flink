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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ApplicationOptionsInternal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link ApplicationJobUtils}. */
public class ApplicationJobUtilsTest {

    private Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new Configuration();
    }

    @Test
    void testMaybeFixJobId_HAEnabled_NoFixedJobId() {
        final String clusterId = "cluster";
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        assertNull(configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));

        ApplicationJobUtils.maybeFixJobIdAndApplicationId(configuration);

        assertEquals(
                new JobID(clusterId.hashCode(), 0L).toHexString(),
                configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
    }

    @Test
    void testMaybeFixJobId_HADisabled_NoFixedJobId() {
        assertEquals(
                HighAvailabilityMode.NONE.name(),
                configuration.get(HighAvailabilityOptions.HA_MODE));
        assertNull(configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));

        ApplicationJobUtils.maybeFixJobIdAndApplicationId(configuration);

        assertNull(configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
    }

    @Test
    void testMaybeFixJobId_HAEnabled_FixedJobIdAlreadySet() {
        final String clusterId = "cluster";
        final JobID testJobID = new JobID(0, 2);
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());

        ApplicationJobUtils.maybeFixJobIdAndApplicationId(configuration);

        assertEquals(
                testJobID.toHexString(),
                configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
    }

    @Test
    void testMaybeFixApplicationId_HAEnabled_NoFixedApplicationId() {
        final String clusterId = "cluster";
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        assertNull(configuration.get(ApplicationOptionsInternal.FIXED_APPLICATION_ID));

        ApplicationJobUtils.maybeFixJobIdAndApplicationId(configuration);

        assertEquals(
                new JobID(clusterId.hashCode(), 0L).toHexString(),
                configuration.get(ApplicationOptionsInternal.FIXED_APPLICATION_ID));
    }

    @Test
    void testMaybeFixApplicationId_HADisabled_NoFixedApplicationId() {
        assertEquals(
                HighAvailabilityMode.NONE.name(),
                configuration.get(HighAvailabilityOptions.HA_MODE));
        assertNull(configuration.get(ApplicationOptionsInternal.FIXED_APPLICATION_ID));

        ApplicationJobUtils.maybeFixJobIdAndApplicationId(configuration);

        assertNull(configuration.get(ApplicationOptionsInternal.FIXED_APPLICATION_ID));
    }

    @Test
    void testMaybeFixApplicationId_HAEnabled_FixedApplicationIdAlreadySet() {
        final String clusterId = "cluster";
        final JobID testJobID = new JobID(0, 2);
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        configuration.set(ApplicationOptionsInternal.FIXED_APPLICATION_ID, testJobID.toHexString());

        ApplicationJobUtils.maybeFixJobIdAndApplicationId(configuration);

        assertEquals(
                testJobID.toHexString(),
                configuration.get(ApplicationOptionsInternal.FIXED_APPLICATION_ID));
    }

    @Test
    void testAllowExecuteMultipleJobs_HADisabled_NoFixedJobId() {
        assertEquals(
                HighAvailabilityMode.NONE.name(),
                configuration.get(HighAvailabilityOptions.HA_MODE));
        assertNull(configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));

        assertTrue(ApplicationJobUtils.allowExecuteMultipleJobs(configuration));
    }

    @Test
    void testAllowExecuteMultipleJobs_HAEnabled_NoFixedJobId() {
        final String clusterId = "cluster";
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        assertNull(configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));

        assertFalse(ApplicationJobUtils.allowExecuteMultipleJobs(configuration));
    }

    @Test
    void testAllowExecuteMultipleJobs_HAEnabled_FixedJobIdSet() {
        final String clusterId = "cluster";
        final JobID testJobID = new JobID(0, 2);
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());

        assertFalse(ApplicationJobUtils.allowExecuteMultipleJobs(configuration));
    }
}
