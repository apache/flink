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

package org.apache.flink.runtime.webmonitor.history.retaining;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_APPLICATIONS;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_TTL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing for {@link CompositeArchiveRetainedStrategy}. */
class CompositeArchiveRetainedStrategyTest {

    private static Stream<TestCase> getTestCases() {
        return Stream.of(
                new TestCase(
                        "Legacy Jobs",
                        HISTORY_SERVER_RETAINED_JOBS,
                        CompositeArchiveRetainedStrategy::createForJobFromConfig),
                new TestCase(
                        "Applications",
                        HISTORY_SERVER_RETAINED_APPLICATIONS,
                        CompositeArchiveRetainedStrategy::createForApplicationFromConfig));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("getTestCases")
    void testTimeToLiveBasedArchiveRetainedStrategy(TestCase testCase) {
        final Configuration conf = new Configuration();

        // Test for invalid option value.
        conf.set(HISTORY_SERVER_RETAINED_TTL, Duration.ZERO);
        assertThatThrownBy(() -> testCase.createStrategy(conf))
                .isInstanceOf(IllegalConfigurationException.class);
        // Skipped for option value that is less than 0 milliseconds, which will throw a
        // java.lang.NumberFormatException caused by TimeUtils.

        conf.removeConfig(HISTORY_SERVER_RETAINED_TTL);

        // Test the case where no specific retention policy is configured, i.e., all archived files
        // are retained.
        ArchiveRetainedStrategy strategy = testCase.createStrategy(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(
                        strategy.shouldRetain(
                                new TestingFileStatus(
                                        Instant.now().toEpochMilli()
                                                - Duration.ofMinutes(1).toMillis()),
                                1))
                .isTrue();

        // Test the case where TTL-based retention policies is specified only.
        conf.set(HISTORY_SERVER_RETAINED_TTL, Duration.ofMinutes(1L));
        strategy = testCase.createStrategy(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(
                        strategy.shouldRetain(
                                new TestingFileStatus(
                                        Instant.now().toEpochMilli()
                                                - Duration.ofMinutes(1).toMillis()),
                                1))
                .isFalse();
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("getTestCases")
    void testQuantityBasedArchiveRetainedStrategy(TestCase testCase) {
        final Configuration conf = new Configuration();

        // Test for invalid option value.
        conf.set(testCase.getQuantityConfigOption(), 0);
        assertThatThrownBy(() -> testCase.createStrategy(conf))
                .isInstanceOf(IllegalConfigurationException.class);
        conf.set(testCase.getQuantityConfigOption(), -2);
        assertThatThrownBy(() -> testCase.createStrategy(conf))
                .isInstanceOf(IllegalConfigurationException.class);

        conf.removeConfig(testCase.getQuantityConfigOption());

        // Test the case where no specific retention policy is configured, i.e., all archived files
        // are retained.
        ArchiveRetainedStrategy strategy = testCase.createStrategy(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 3)).isTrue();

        // Test the case where QUANTITY-based retention policies is specified only.
        conf.set(testCase.getQuantityConfigOption(), 2);
        strategy = testCase.createStrategy(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 3)).isFalse();
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("getTestCases")
    void testCompositeBasedArchiveRetainedStrategy(TestCase testCase) {

        final long outOfTtlMillis =
                Instant.now().toEpochMilli() - Duration.ofMinutes(2L).toMillis();

        // Test the case where no specific retention policy is configured, i.e., all archived files
        // are retained.
        final Configuration conf = new Configuration();
        ArchiveRetainedStrategy strategy = testCase.createStrategy(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 10)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 3)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();

        // Test the case where both retention policies are specified.
        conf.set(HISTORY_SERVER_RETAINED_TTL, Duration.ofMinutes(1));
        conf.set(testCase.getQuantityConfigOption(), 2);
        strategy = testCase.createStrategy(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 1)).isFalse();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 10)).isFalse();
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 3)).isFalse();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
    }

    private static final class TestingFileStatus implements FileStatus {

        private final long modificationTime;

        TestingFileStatus() {
            this(Instant.now().toEpochMilli());
        }

        TestingFileStatus(long modificationTime) {
            this.modificationTime = modificationTime;
        }

        @Override
        public long getLen() {
            return 0;
        }

        @Override
        public long getBlockSize() {
            return 0;
        }

        @Override
        public short getReplication() {
            return 0;
        }

        @Override
        public long getModificationTime() {
            return modificationTime;
        }

        @Override
        public long getAccessTime() {
            return 0;
        }

        @Override
        public boolean isDir() {
            return false;
        }

        @Override
        public Path getPath() {
            return null;
        }
    }

    private static final class TestCase {
        private final String testName;
        private final ConfigOption<Integer> quantityConfigOption;
        private final Function<Configuration, ArchiveRetainedStrategy> strategyFunction;

        TestCase(
                String testName,
                ConfigOption<Integer> quantityConfigOption,
                Function<Configuration, ArchiveRetainedStrategy> strategyFunction) {
            this.testName = testName;
            this.quantityConfigOption = quantityConfigOption;
            this.strategyFunction = strategyFunction;
        }

        ArchiveRetainedStrategy createStrategy(Configuration conf) {
            return strategyFunction.apply(conf);
        }

        ConfigOption<Integer> getQuantityConfigOption() {
            return quantityConfigOption;
        }

        @Override
        public String toString() {
            return testName;
        }
    }
}
