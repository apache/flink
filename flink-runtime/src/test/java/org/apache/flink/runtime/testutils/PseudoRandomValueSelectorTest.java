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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.JobManagerOptions.TOTAL_PROCESS_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.CPU_CORES;
import static org.apache.flink.runtime.testutils.PseudoRandomValueSelector.randomize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** Tests {@link PseudoRandomValueSelector}. */
class PseudoRandomValueSelectorTest {

    /**
     * Tests that the selector will return different values if invoked several times even for the
     * same option.
     */
    @Test
    void testRandomizationOfValues() {
        final Double[] alternatives =
                IntStream.range(0, 1000).boxed().map(Double::valueOf).toArray(Double[]::new);

        final PseudoRandomValueSelector valueSelector = PseudoRandomValueSelector.create("seed");

        final Set<Double> uniqueValues = new HashSet<>(1);
        for (int i = 0; i < 100; i++) {
            final Double selectedValue = selectValue(valueSelector, CPU_CORES, alternatives);
            uniqueValues.add(selectedValue);
        }
        assertThat(uniqueValues).hasSizeGreaterThan(1);
    }

    private <T> T selectValue(
            PseudoRandomValueSelector valueSelector, ConfigOption<T> option, T... alternatives) {
        final Configuration configuration = new Configuration();
        assertThat(configuration.get(option)).isNull();
        valueSelector.select(configuration, option, alternatives);
        final T selected = configuration.get(option);
        assertThat(selected).isNotNull();
        return selected;
    }

    /** Tests that the selector will return different values for different seeds. */
    @Test
    void testRandomizationWithSeed() {
        final Double[] alternatives =
                IntStream.range(0, 1000).boxed().map(Double::valueOf).toArray(Double[]::new);

        final Set<Double> uniqueValues = new HashSet<>(1);
        for (int i = 0; i < 100; i++) {
            final PseudoRandomValueSelector selector = PseudoRandomValueSelector.create("test" + i);
            uniqueValues.add(selectValue(selector, CPU_CORES, alternatives));
        }
        assertThat(uniqueValues).hasSizeGreaterThan(1);
    }

    /** Tests that the selector produces the same value for the same seed. */
    @Test
    void testStableRandomization() {
        final Double[] doubles =
                IntStream.range(0, 1000).boxed().map(Double::valueOf).toArray(Double[]::new);
        final MemorySize[] memorySizes =
                IntStream.range(0, 1000)
                        .mapToObj(MemorySize::ofMebiBytes)
                        .toArray(MemorySize[]::new);
        final String[] strings =
                IntStream.range(0, 1000).mapToObj(i -> "string" + i).toArray(String[]::new);

        final Set<Tuple3<Double, MemorySize, String>> uniqueValues = new HashSet<>(1);
        for (int i = 0; i < 100; i++) {
            final PseudoRandomValueSelector selector = PseudoRandomValueSelector.create("test");
            uniqueValues.add(
                    new Tuple3<>(
                            selectValue(selector, CPU_CORES, doubles),
                            selectValue(selector, TOTAL_PROCESS_MEMORY, memorySizes),
                            selectValue(selector, SAVEPOINT_DIRECTORY, strings)));
        }
        assertThat(uniqueValues).hasSize(1);
    }

    /**
     * Tests that randomize() produces different values for different config options within the same
     * test. Previously, all boolean options got the same value because the seed did not include the
     * config option key.
     */
    @Test
    void testRandomizeDifferentOptionsProduceDifferentValues() {
        final Integer[] alternatives = IntStream.range(0, 20).boxed().toArray(Integer[]::new);

        final Configuration conf = new Configuration();
        final Set<Integer> uniqueValues = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ConfigOption<Integer> option =
                    ConfigOptions.key("test.randomize.option." + i).intType().noDefaultValue();
            randomize(conf, option, alternatives);
            uniqueValues.add(conf.get(option));
        }
        // 100 independent nextInt(20) calls should produce many distinct values
        assertThat(uniqueValues).hasSizeGreaterThan(15);
    }

    /**
     * Tests that reading through git command yields the same as {@link EnvironmentInformation}.
     *
     * <p>This test assumes that both sources of information are available (CI).
     */
    @Test
    void readCommitId() {
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
        assertThat(gitCommitId).isPresent().contains(envCommitId);
    }
}
