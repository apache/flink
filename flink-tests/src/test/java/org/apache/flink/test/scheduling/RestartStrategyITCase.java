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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.EXPONENTIAL_DELAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for restart strategy. */
class RestartStrategyITCase {

    private static Stream<Arguments> provideRuntimeModeAndSchedulerParameters() {
        return Stream.of(
                Arguments.of(
                        RuntimeExecutionMode.STREAMING, JobManagerOptions.SchedulerType.Default),
                Arguments.of(
                        RuntimeExecutionMode.STREAMING, JobManagerOptions.SchedulerType.Adaptive),
                Arguments.of(RuntimeExecutionMode.BATCH, JobManagerOptions.SchedulerType.Default),
                Arguments.of(
                        RuntimeExecutionMode.BATCH, JobManagerOptions.SchedulerType.AdaptiveBatch));
    }

    @ParameterizedTest
    @MethodSource("provideRuntimeModeAndSchedulerParameters")
    void testExponentialDelayRestartStrategyAttempts(
            RuntimeExecutionMode runtimeExecutionMode,
            JobManagerOptions.SchedulerType schedulerType)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, EXPONENTIAL_DELAY.getMainValue());
        conf.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
                Duration.ofSeconds(1));
        conf.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF,
                Duration.ofSeconds(3));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS, 3);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(runtimeExecutionMode);
        conf.set(JobManagerOptions.SCHEDULER, schedulerType);

        env.setParallelism(1);
        env.configure(conf);

        FailureCountableSink<Long> failureCountableSink = new FailureCountableSink<>();

        env.fromSource(
                        new DataGeneratorSource<>(
                                value -> value,
                                3000,
                                RateLimiterStrategy.perSecond(100),
                                Types.LONG),
                        WatermarkStrategy.noWatermarks(),
                        "Data Generator")
                .addSink(failureCountableSink)
                .name("MySink");

        JobClient jobClient = env.executeAsync();

        assertThatThrownBy(() -> jobClient.getJobExecutionResult().get())
                .cause()
                .cause()
                .hasMessageContaining(
                        "Recovery is suppressed by ExponentialDelayRestartBackoffTimeStrategy")
                .hasRootCauseMessage("Expected exception.");

        assertThat(failureCountableSink.getFailureCounter()).isEqualTo(4);
    }

    /** Throwing exception directly when processing data, and count all exceptions. */
    private static class FailureCountableSink<IN> extends RichSinkFunction<IN> {

        private static final AtomicLong failureCounter = new AtomicLong();

        public FailureCountableSink() {
            failureCounter.set(0);
        }

        @Override
        public void invoke(IN value, SinkFunction.Context context) {
            failureCounter.incrementAndGet();
            throw new RuntimeException("Expected exception.");
        }

        public long getFailureCounter() {
            return failureCounter.get();
        }
    }
}
