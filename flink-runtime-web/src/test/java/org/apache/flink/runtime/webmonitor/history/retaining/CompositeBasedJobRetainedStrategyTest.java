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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS_MODE;
import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS_MODE_THRESHOLDS;
import static org.apache.flink.configuration.HistoryServerOptions.JobArchivedRetainedMode;
import static org.apache.flink.runtime.webmonitor.history.retaining.CompositeBasedJobRetainedStrategy.LogicEvaluate.AND;
import static org.apache.flink.runtime.webmonitor.history.retaining.CompositeBasedJobRetainedStrategy.LogicEvaluate.OR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing for {@link CompositeBasedJobRetainedStrategy}. */
class CompositeBasedJobRetainedStrategyTest {

    @Test
    void testCompatibilityBetweenOldSizedAndModeBasedJobRetainedStrategy() {
        final Configuration confWithoutNewOptionValue = new Configuration();
        CompositeBasedJobRetainedStrategy targetJobRetainedStrategy;

        targetJobRetainedStrategy =
                (CompositeBasedJobRetainedStrategy)
                        CompositeBasedJobRetainedStrategy.createFrom(confWithoutNewOptionValue);
        assertThat(targetJobRetainedStrategy.getStrategies()).isEmpty();

        confWithoutNewOptionValue.set(HISTORY_SERVER_RETAINED_JOBS, -2);
        assertIllegalConfigurationException(confWithoutNewOptionValue);

        confWithoutNewOptionValue.set(HISTORY_SERVER_RETAINED_JOBS, 0);
        assertIllegalConfigurationException(confWithoutNewOptionValue);

        confWithoutNewOptionValue.set(HISTORY_SERVER_RETAINED_JOBS, 1);
        assertOnlyDeprecatedOptionEvaluateLogic(
                confWithoutNewOptionValue, QuantityBasedJobRetainedStrategy.class);

        Configuration confWithNewOptionValue = new Configuration();
        confWithNewOptionValue.set(
                HISTORY_SERVER_RETAINED_JOBS_MODE, JobArchivedRetainedMode.Quantity);
        assertOnlyDeprecatedOptionEvaluateLogic(
                confWithNewOptionValue, QuantityBasedJobRetainedStrategy.class);

        confWithNewOptionValue.set(HISTORY_SERVER_RETAINED_JOBS_MODE, JobArchivedRetainedMode.Ttl);
        assertOnlyDeprecatedOptionEvaluateLogic(
                confWithNewOptionValue, TimeToLiveBasedJobRetainedStrategy.class);

        confWithNewOptionValue.set(
                HISTORY_SERVER_RETAINED_JOBS_MODE, JobArchivedRetainedMode.TtlAndQuantity);
        assertComposedStrategies(confWithNewOptionValue, AND);

        confWithNewOptionValue.set(
                HISTORY_SERVER_RETAINED_JOBS_MODE, JobArchivedRetainedMode.TtlOrQuantity);
        assertComposedStrategies(confWithNewOptionValue, OR);
    }

    private static void assertComposedStrategies(
            Configuration confWithNewOptionValue,
            CompositeBasedJobRetainedStrategy.LogicEvaluate logicEvaluate) {
        CompositeBasedJobRetainedStrategy targetJobRetainedStrategy;
        targetJobRetainedStrategy =
                (CompositeBasedJobRetainedStrategy)
                        CompositeBasedJobRetainedStrategy.createFrom(confWithNewOptionValue);
        assertThat(targetJobRetainedStrategy.getStrategies())
                .hasSize(2)
                .hasOnlyElementsOfTypes(
                        QuantityBasedJobRetainedStrategy.class,
                        TimeToLiveBasedJobRetainedStrategy.class);
        assertThat(targetJobRetainedStrategy.getLogicEvaluate()).isEqualTo(logicEvaluate);
    }

    private static void assertOnlyDeprecatedOptionEvaluateLogic(
            Configuration confWithoutNewOptionValue, Class<?> targetStrategyClass) {
        CompositeBasedJobRetainedStrategy targetJobRetainedStrategy;
        targetJobRetainedStrategy =
                (CompositeBasedJobRetainedStrategy)
                        CompositeBasedJobRetainedStrategy.createFrom(confWithoutNewOptionValue);
        assertThat(targetJobRetainedStrategy.getStrategies())
                .hasSize(1)
                .hasExactlyElementsOfTypes(targetStrategyClass);
    }

    private static void assertIllegalConfigurationException(
            Configuration confWithoutNewOptionValue) {
        assertThatThrownBy(
                        () ->
                                CompositeBasedJobRetainedStrategy.createFrom(
                                        confWithoutNewOptionValue))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testTimeToLiveBasedJobRetainedStrategy() {
        Map<String, String> props = new HashMap<>();
        JobArchivesRetainedStrategy strategy = new TimeToLiveBasedJobRetainedStrategy(props);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(
                        strategy.shouldRetain(
                                new TestingFileStatus(
                                        Instant.now().toEpochMilli()
                                                - Duration.ofMinutes(1).toMillis()),
                                1))
                .isTrue();

        props = Map.of("ttl", "1min");
        strategy = new TimeToLiveBasedJobRetainedStrategy(props);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(
                        strategy.shouldRetain(
                                new TestingFileStatus(
                                        Instant.now().toEpochMilli()
                                                - Duration.ofMinutes(1).toMillis()),
                                1))
                .isFalse();
    }

    @Test
    void testQuantityBasedJobRetainedStrategy() {
        Map<String, String> props = new HashMap<>();
        JobArchivesRetainedStrategy strategy = new TimeToLiveBasedJobRetainedStrategy(props);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 10)).isTrue();

        props = Map.of("quantity", "2");
        strategy = new QuantityBasedJobRetainedStrategy(props);
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 3)).isFalse();
    }

    @Test
    void testCompositeBasedJobRetainedStrategy() {

        final long outOfTtlMillis =
                Instant.now().toEpochMilli() - Duration.ofMinutes(2L).toMillis();

        final Configuration conf = new Configuration();
        conf.set(
                HISTORY_SERVER_RETAINED_JOBS_MODE_THRESHOLDS,
                Map.of("quantity", "2", "ttl", "1min"));
        conf.set(HISTORY_SERVER_RETAINED_JOBS_MODE, JobArchivedRetainedMode.TtlAndQuantity);
        JobArchivesRetainedStrategy strategy = CompositeBasedJobRetainedStrategy.createFrom(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 1)).isFalse();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 10)).isFalse();
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 3)).isFalse();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();

        conf.set(HISTORY_SERVER_RETAINED_JOBS_MODE, JobArchivedRetainedMode.TtlOrQuantity);
        strategy = CompositeBasedJobRetainedStrategy.createFrom(conf);
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 3)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(), 1)).isTrue();
        assertThat(strategy.shouldRetain(new TestingFileStatus(outOfTtlMillis), 3)).isFalse();
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
}
