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

package org.apache.flink.core.testutils;

import org.apache.flink.FlinkVersion;

import org.junit.Assume;

import java.util.Collection;
import java.util.Set;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * {@code FlinkVerionBasedTestDataGenerationUtils} collects functionality around generating test
 * data for migration tests.
 */
public class FlinkVersionBasedTestDataGenerationUtils {

    /**
     * Run the test suite on the corresponding release branch and enable the test data generation by
     * setting {@code GENERATE_MIGRATION_TEST_DATA_PROPERTY} as a system property.
     *
     * <p>{@link #mostRecentlyPublishedBaseMinorVersion()} needs to match the version specified in
     * the system property.
     */
    public static final String GENERATE_MIGRATION_TEST_DATA_PROPERTY =
            "generate-migration-test-data";

    /**
     * Some migration tests rely on snapshots being generated after the initial version of a new
     * major/minor version (i.e. x.y.0) was released. These tests will rely on this method to return
     * upper threshold of the relevant version range.
     *
     * <p>Usually, related tests will fail due to the missing snapshot data. Upgrading this method's
     * return value would require generating the snapshot data for the corresponding tests. See
     * {@code FlinkVersionBasedTestDataGenerationUtils} in the {@code flink-test-utils} module for
     * further details on how to generate the test data.
     */
    public static FlinkVersion mostRecentlyPublishedBaseMinorVersion() {
        return FlinkVersion.v1_15;
    }

    /**
     * Creates an {@code Iterable} of {@link FlinkVersion} instances starting from {@code
     * minInclVersion} up to {@link #mostRecentlyPublishedBaseMinorVersion()}.
     *
     * @param minInclVersion The smallest minor version that should be part of the range.
     * @return A {@code Collection} (instead of {@code Iterable}) to support JUnit4
     *     {@code @Parameterized.Parameters}.
     */
    public static Collection<FlinkVersion> rangeFrom(FlinkVersion minInclVersion) {
        return FlinkVersion.rangeOf(minInclVersion, mostRecentlyPublishedBaseMinorVersion());
    }

    /**
     * Creates an {@code Iterable} of {@link FlinkVersion} instances starting from {@code
     * minInclVersion} up to {@link #mostRecentlyPublishedBaseMinorVersion()} without the {@code
     * FlinkVersions} that are meant to be excluded from this range.
     *
     * @param minInclVersion The smallest minor version that should be part of the range.
     * @param excludedVersions {@code FlinkVersion} instances that would fall into the range but
     *     should be excluded.
     * @return A {@code Collection} (instead of {@code Iterable}) to support JUnit4
     *     {@code @Parameterized.Parameters}.
     */
    public static Collection<FlinkVersion> rangeFromVersionExcludingIntermediates(
            FlinkVersion minInclVersion, FlinkVersion... excludedVersions) {
        final Set<FlinkVersion> versions =
                FlinkVersion.rangeOf(minInclVersion, mostRecentlyPublishedBaseMinorVersion());
        for (FlinkVersion excludedVersion : excludedVersions) {
            versions.remove(excludedVersion);
        }

        return versions;
    }

    public static void assumeFlinkVersionWithDescriptiveTextMessageJUnit4(
            FlinkVersion flinkVersionOfRun) {
        Assume.assumeTrue(
                getAssumptionMessageForDisabledTestDataGeneration(),
                testDataGenerationEnabledFor(flinkVersionOfRun));
    }

    public static void assumeFlinkVersionBasedTestDataGenerationDisabledJUnit4() {
        Assume.assumeTrue(
                getAssumptionMessageForEnabledTestDataGeneration(), testDataGenerationDisabled());
    }

    public static void assumeFlinkVersionWithDescriptiveMessage(FlinkVersion flinkVersionOfRun) {
        assumeThat(testDataGenerationEnabledFor(flinkVersionOfRun))
                .as(getAssumptionMessageForDisabledTestDataGeneration());
    }

    public static void assumeFlinkVersionBasedTestDataGenerationDisabled() {
        assumeThat(testDataGenerationDisabled())
                .as(getAssumptionMessageForDisabledTestDataGeneration());
    }

    private static boolean testDataGenerationDisabled() {
        return System.getProperty(GENERATE_MIGRATION_TEST_DATA_PROPERTY) == null;
    }

    public static boolean testDataGenerationEnabledFor(FlinkVersion flinkVersion) {
        return FlinkVersion.byCode(System.getProperty(GENERATE_MIGRATION_TEST_DATA_PROPERTY))
                .filter(v -> v == flinkVersion)
                .isPresent();
    }

    public static String getAssumptionMessageForDisabledTestDataGeneration() {
        return "Generating test data is disabled due to "
                + GENERATE_MIGRATION_TEST_DATA_PROPERTY
                + " not being set as a system property.";
    }

    private static String getAssumptionMessageForEnabledTestDataGeneration() {
        return "Generating test data is enabled (due to "
                + GENERATE_MIGRATION_TEST_DATA_PROPERTY
                + " being set as a system property) which makes this test being skipped.";
    }
}
