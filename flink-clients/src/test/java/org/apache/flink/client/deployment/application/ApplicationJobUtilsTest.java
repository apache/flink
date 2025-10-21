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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.AbstractID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link ApplicationJobUtils}. */
public class ApplicationJobUtilsTest {

    private static final String TEST_HA_CLUSTER_ID = "cluster";
    private static final String TEST_CLUSTER_ID = "3dc42a26ed5afedb8c6e1132809dcf73";
    private static final String TEST_APPLICATION_ID = "ca0eb040022fbccd4cf05d1e274ae25e";
    private static final String TEST_JOB_ID = "e79b6d171acd4baa6f421e3631168810";

    private Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new Configuration();
    }

    @ParameterizedTest
    @MethodSource("provideParametersForMaybeFixIds")
    void testMaybeFixIds(
            boolean isHAEnabled,
            boolean isHaClusterIdSet,
            boolean isClusterIdSet,
            boolean isApplicationIdSet,
            boolean isJobIdSet,
            @Nullable String expectedClusterId,
            @Nullable String expectedApplicationId,
            @Nullable String expectedJobId) {
        if (isHAEnabled) {
            configuration.set(
                    HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        }
        if (isHaClusterIdSet) {
            configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, TEST_HA_CLUSTER_ID);
        }
        if (isClusterIdSet) {
            configuration.set(ClusterOptions.CLUSTER_ID, TEST_CLUSTER_ID);
        }
        if (isApplicationIdSet) {
            configuration.set(ApplicationOptionsInternal.FIXED_APPLICATION_ID, TEST_APPLICATION_ID);
        }
        if (isJobIdSet) {
            configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, TEST_JOB_ID);
        }

        ApplicationJobUtils.maybeFixIds(configuration);

        assertEquals(expectedClusterId, configuration.get(ClusterOptions.CLUSTER_ID));
        assertEquals(
                expectedApplicationId,
                configuration.get(ApplicationOptionsInternal.FIXED_APPLICATION_ID));
        assertEquals(
                expectedJobId, configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
    }

    private static Stream<Arguments> provideParametersForMaybeFixIds() {
        // all combinations for the five input: (isHAEnabled, isHaClusterIdSet, isClusterIdSet,
        // isApplicationIdSet, isJobIdSet)
        return Stream.of(
                Arguments.of(
                        false,
                        false,
                        false,
                        false,
                        false,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        null,
                        null),
                Arguments.of(
                        false,
                        false,
                        false,
                        false,
                        true,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        null,
                        TEST_JOB_ID),
                Arguments.of(
                        false,
                        false,
                        false,
                        true,
                        false,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        TEST_APPLICATION_ID,
                        null),
                Arguments.of(
                        false,
                        false,
                        false,
                        true,
                        true,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(false, false, true, false, false, TEST_CLUSTER_ID, null, null),
                Arguments.of(false, false, true, false, true, TEST_CLUSTER_ID, null, TEST_JOB_ID),
                Arguments.of(
                        false,
                        false,
                        true,
                        true,
                        false,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        null),
                Arguments.of(
                        false,
                        false,
                        true,
                        true,
                        true,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        false,
                        true,
                        false,
                        false,
                        false,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        null,
                        null),
                Arguments.of(
                        false,
                        true,
                        false,
                        false,
                        true,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        null,
                        TEST_JOB_ID),
                Arguments.of(
                        false,
                        true,
                        false,
                        true,
                        false,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        TEST_APPLICATION_ID,
                        null),
                Arguments.of(
                        false,
                        true,
                        false,
                        true,
                        true,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(false, true, true, false, false, TEST_CLUSTER_ID, null, null),
                Arguments.of(false, true, true, false, true, TEST_CLUSTER_ID, null, TEST_JOB_ID),
                Arguments.of(
                        false, true, true, true, false, TEST_CLUSTER_ID, TEST_APPLICATION_ID, null),
                Arguments.of(
                        false,
                        true,
                        true,
                        true,
                        true,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        false,
                        false,
                        false,
                        false,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        getAbstractIdFromString(
                                HighAvailabilityOptions.HA_CLUSTER_ID.defaultValue())),
                Arguments.of(
                        true,
                        false,
                        false,
                        false,
                        true,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        false,
                        false,
                        true,
                        false,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        TEST_APPLICATION_ID,
                        TEST_APPLICATION_ID),
                Arguments.of(
                        true,
                        false,
                        false,
                        true,
                        true,
                        ClusterOptions.CLUSTER_ID.defaultValue(),
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        false,
                        true,
                        false,
                        false,
                        TEST_CLUSTER_ID,
                        TEST_CLUSTER_ID,
                        TEST_CLUSTER_ID),
                Arguments.of(
                        true,
                        false,
                        true,
                        false,
                        true,
                        TEST_CLUSTER_ID,
                        TEST_CLUSTER_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        false,
                        true,
                        true,
                        false,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        TEST_APPLICATION_ID),
                Arguments.of(
                        true,
                        false,
                        true,
                        true,
                        true,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        true,
                        false,
                        false,
                        false,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID)),
                Arguments.of(
                        true,
                        true,
                        false,
                        false,
                        true,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        true,
                        false,
                        true,
                        false,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        TEST_APPLICATION_ID,
                        TEST_APPLICATION_ID),
                Arguments.of(
                        true,
                        true,
                        false,
                        true,
                        true,
                        getAbstractIdFromString(TEST_HA_CLUSTER_ID),
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        true,
                        true,
                        false,
                        false,
                        TEST_CLUSTER_ID,
                        TEST_CLUSTER_ID,
                        TEST_CLUSTER_ID),
                Arguments.of(
                        true,
                        true,
                        true,
                        false,
                        true,
                        TEST_CLUSTER_ID,
                        TEST_CLUSTER_ID,
                        TEST_JOB_ID),
                Arguments.of(
                        true,
                        true,
                        true,
                        true,
                        false,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        TEST_APPLICATION_ID),
                Arguments.of(
                        true,
                        true,
                        true,
                        true,
                        true,
                        TEST_CLUSTER_ID,
                        TEST_APPLICATION_ID,
                        TEST_JOB_ID));
    }

    private static String getAbstractIdFromString(String str) {
        return (new AbstractID(str.hashCode(), 0)).toHexString();
    }

    @Test
    void testMaybeFixIds_ClusterIdMalformed() {
        final String clusterId = "cluster";
        configuration.set(ClusterOptions.CLUSTER_ID, clusterId);

        assertThrows(
                IllegalArgumentException.class,
                () -> ApplicationJobUtils.maybeFixIds(configuration));
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
