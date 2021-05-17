/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeNotNull;

/** Tests {@link PseudoRandomValueSelector}. */
public class PseudoRandomValueSelectorTest extends TestLogger {

    /**
     * Tests that the selector will return different values if invoked several times even for the
     * same option.
     */
    @Test
    public void testRandomizationOfValues() {
        final Duration[] alternatives =
                IntStream.range(0, 1000).boxed().map(Duration::ofMillis).toArray(Duration[]::new);

        final PseudoRandomValueSelector valueSelector = PseudoRandomValueSelector.create("seed");

        final Set<Duration> uniqueValues = new HashSet<>(1);
        for (int i = 0; i < 100; i++) {
            final Duration selectedValue =
                    selectValue(valueSelector, CHECKPOINTING_INTERVAL, alternatives);
            uniqueValues.add(selectedValue);
        }
        assertThat(uniqueValues.size(), greaterThan(1));
    }

    private <T> T selectValue(
            PseudoRandomValueSelector valueSelector, ConfigOption<T> option, T... alternatives) {
        final Configuration configuration = new Configuration();
        assertNull(configuration.get(option));
        valueSelector.select(configuration, option, alternatives);
        final T selected = configuration.get(option);
        assertNotNull(selected);
        return selected;
    }

    /** Tests that the selector will return different values for different seeds. */
    @Test
    public void testRandomizationWithSeed() {
        final Duration[] alternatives =
                IntStream.range(0, 1000).boxed().map(Duration::ofMillis).toArray(Duration[]::new);

        final Set<Duration> uniqueValues = new HashSet<>(1);
        for (int i = 0; i < 100; i++) {
            final PseudoRandomValueSelector selector = PseudoRandomValueSelector.create("test" + i);
            uniqueValues.add(selectValue(selector, CHECKPOINTING_INTERVAL, alternatives));
        }
        assertThat(uniqueValues.size(), greaterThan(1));
    }

    /** Tests that the selector produces the same value for the same seed. */
    @Test
    public void testStableRandomization() {
        final Duration[] intervals =
                IntStream.range(0, 1000).boxed().map(Duration::ofMillis).toArray(Duration[]::new);
        final Integer[] numbers = IntStream.range(0, 1000).boxed().toArray(Integer[]::new);
        final String[] strings =
                IntStream.range(0, 1000).mapToObj(i -> "string" + i).toArray(String[]::new);

        final Set<Tuple3<Duration, Integer, String>> uniqueValues = new HashSet<>(1);
        for (int i = 0; i < 100; i++) {
            final PseudoRandomValueSelector selector = PseudoRandomValueSelector.create("test");
            uniqueValues.add(
                    new Tuple3<>(
                            selectValue(selector, CHECKPOINTING_INTERVAL, intervals),
                            selectValue(selector, TOLERABLE_FAILURE_NUMBER, numbers),
                            selectValue(selector, SAVEPOINT_DIRECTORY, strings)));
        }
        assertEquals(1, uniqueValues.size());
    }

    /**
     * Tests that reading through git command yields the same as {@link EnvironmentInformation}.
     *
     * <p>This test assumes that both sources of information are available (CI).
     */
    @Test
    public void readCommitId() {
        assumeNotNull(ZooKeeperTestUtils.runsOnCIInfrastructure());
        // this information is only valid after executing process-resources on flink-runtime
        final String envCommitId = EnvironmentInformation.getGitCommitId();
        assumeFalse(envCommitId.equals(EnvironmentInformation.UNKNOWN_COMMIT_ID));
        // test if git is available
        try {
            new ProcessBuilder("git", "version").start();
        } catch (IOException e) {
            assumeNoException(e);
        }

        final Optional<String> gitCommitId = PseudoRandomValueSelector.getGitCommitId();
        assertTrue(gitCommitId.isPresent());
        assertEquals(envCommitId, gitCommitId.get());
    }
}
