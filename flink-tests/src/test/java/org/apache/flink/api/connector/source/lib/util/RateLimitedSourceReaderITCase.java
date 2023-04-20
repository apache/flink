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

package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** An integration test for rate limiting built into the DataGeneratorSource. */
public class RateLimitedSourceReaderITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @RegisterExtension
    private static final MiniClusterExtension miniClusterExtension =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    @DisplayName("Rate limiter is used correctly.")
    public void testRateLimitingParallelExecution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        final int count = 10;

        final MockRateLimiterStrategy rateLimiterStrategy = new MockRateLimiterStrategy();

        final DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>(index -> index, 10, rateLimiterStrategy, Types.LONG);

        final DataStream<Long> stream =
                env.fromSource(
                        dataGeneratorSource, WatermarkStrategy.noWatermarks(), "generator source");

        final List<Long> result = stream.executeAndCollect(10000);
        int rateLimiterCallCount = MockRateLimiterStrategy.getRateLimitersCallCount();

        assertThat(result).containsExactlyInAnyOrderElementsOf(range(0, 9));
        assertThat(rateLimiterCallCount).isGreaterThan(count);
    }

    private List<Long> range(int startInclusive, int endInclusive) {
        return LongStream.rangeClosed(startInclusive, endInclusive)
                .boxed()
                .collect(Collectors.toList());
    }

    private static final class MockRateLimiter implements RateLimiter {

        int callCount;

        @Override
        public CompletableFuture<Void> acquire() {
            callCount++;
            return CompletableFuture.completedFuture(null);
        }

        public int getCallCount() {
            return callCount;
        }
    }

    private static class MockRateLimiterStrategy implements RateLimiterStrategy {

        private static final List<MockRateLimiter> rateLimiters =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public RateLimiter createRateLimiter(int parallelism) {
            MockRateLimiter mockRateLimiter = new MockRateLimiter();
            rateLimiters.add(mockRateLimiter);
            return mockRateLimiter;
        }

        public static int getRateLimitersCallCount() {
            return rateLimiters.stream().mapToInt(MockRateLimiter::getCallCount).sum();
        }
    }
}
