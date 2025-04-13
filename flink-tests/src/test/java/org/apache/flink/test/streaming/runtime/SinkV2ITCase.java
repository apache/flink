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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.GatedRateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.Record;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.RecordSerializer;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link org.apache.flink.api.connector.sink2.Sink} run time implementation.
 */
public class SinkV2ITCase extends AbstractTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(SinkV2ITCase.class);

    static final List<Integer> SOURCE_DATA =
            Arrays.asList(
                    895, 127, 148, 161, 148, 662, 822, 491, 275, 122, 850, 630, 682, 765, 434, 970,
                    714, 795, 288, 422);

    static final List<Record<Integer>> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    // source send data two times
                    .flatMap(
                            x ->
                                    Collections.nCopies(2, new Record<>(x, null, Long.MIN_VALUE))
                                            .stream())
                    .collect(Collectors.toList());

    static final List<Record<Integer>> EXPECTED_COMMITTED_DATA_IN_BATCH_MODE =
            SOURCE_DATA.stream()
                    .map(x -> new Record<>(x, null, Long.MIN_VALUE))
                    .collect(Collectors.toList());

    @RegisterExtension
    static final SharedObjectsExtension SHARED_OBJECTS = SharedObjectsExtension.create();

    @Test
    public void writerAndCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();
        SharedReference<Queue<Committer.CommitRequest<Record<Integer>>>> committed =
                SHARED_OBJECTS.add(new ConcurrentLinkedQueue<>());
        final Source<Integer, ?, ?> source = createStreamingSource();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setCommitter(
                                        new TrackingCommitter(committed), RecordSerializer::new)
                                .build());
        env.execute();
        assertThat(committed.get())
                .extracting(Committer.CommitRequest::getCommittable)
                .containsExactlyInAnyOrderElementsOf(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE);
    }

    @Test
    public void writerAndPrecommitToplogyAndCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();
        SharedReference<Queue<Committer.CommitRequest<Record<Integer>>>> committed =
                SHARED_OBJECTS.add(new ConcurrentLinkedQueue<>());
        final Source<Integer, ?, ?> source = createStreamingSource();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setCommitter(
                                        new TrackingCommitter(committed), RecordSerializer::new)
                                .setWithPreCommitTopology(SinkV2ITCase::flipValue)
                                .build());
        env.execute();
        assertThat(committed.get())
                .extracting(Committer.CommitRequest::getCommittable)
                .containsExactlyInAnyOrderElementsOf(
                        EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.stream()
                                .map(SinkV2ITCase::flipValue)
                                .collect(Collectors.toList()));
    }

    private static Record<Integer> flipValue(Record<Integer> r) {
        return r.withValue(-r.getValue());
    }

    @Test
    public void writerAndCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();
        SharedReference<Queue<Committer.CommitRequest<Record<Integer>>>> committed =
                SHARED_OBJECTS.add(new ConcurrentLinkedQueue<>());

        final DataGeneratorSource<Integer> source =
                new DataGeneratorSource<>(
                        l -> SOURCE_DATA.get(l.intValue()),
                        SOURCE_DATA.size(),
                        IntegerTypeInfo.INT_TYPE_INFO);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setCommitter(
                                        new TrackingCommitter(committed), RecordSerializer::new)
                                .build());
        env.execute();
        assertThat(committed.get())
                .extracting(Committer.CommitRequest::getCommittable)
                .containsExactlyInAnyOrderElementsOf(EXPECTED_COMMITTED_DATA_IN_BATCH_MODE);
    }

    @Test
    public void writerAndPrecommitToplogyAndCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();
        SharedReference<Queue<Committer.CommitRequest<Record<Integer>>>> committed =
                SHARED_OBJECTS.add(new ConcurrentLinkedQueue<>());

        final DataGeneratorSource<Integer> source =
                new DataGeneratorSource<>(
                        l -> SOURCE_DATA.get(l.intValue()),
                        SOURCE_DATA.size(),
                        IntegerTypeInfo.INT_TYPE_INFO);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .sinkTo(
                        TestSinkV2.<Integer>newBuilder()
                                .setCommitter(
                                        new TrackingCommitter(committed), RecordSerializer::new)
                                .setWithPreCommitTopology(SinkV2ITCase::flipValue)
                                .build());
        env.execute();
        assertThat(committed.get())
                .extracting(Committer.CommitRequest::getCommittable)
                .containsExactlyInAnyOrderElementsOf(
                        EXPECTED_COMMITTED_DATA_IN_BATCH_MODE.stream()
                                .map(SinkV2ITCase::flipValue)
                                .collect(Collectors.toList()));
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        return env;
    }

    private StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }

    /**
     * A stream source that: 1) emits a list of elements without allowing checkpoints, 2) then waits
     * for two more checkpoints to complete, 3) then re-emits the same elements before 4) waiting
     * for another two checkpoints and 5) exiting.
     */
    private Source<Integer, ?, ?> createStreamingSource() {
        RateLimiterStrategy rateLimiterStrategy =
                parallelism -> new BurstingRateLimiter(SOURCE_DATA.size() / 4, 2);
        return new DataGeneratorSource<>(
                l -> SOURCE_DATA.get(l.intValue() % SOURCE_DATA.size()),
                SOURCE_DATA.size() * 2L,
                rateLimiterStrategy,
                IntegerTypeInfo.INT_TYPE_INFO);
    }

    private static class BurstingRateLimiter implements RateLimiter {
        private final RateLimiter rateLimiter;
        private final int numCheckpointCooldown;
        private int cooldown;

        public BurstingRateLimiter(int recordPerCycle, int numCheckpointCooldown) {
            rateLimiter = new GatedRateLimiter(recordPerCycle);
            this.numCheckpointCooldown = numCheckpointCooldown;
        }

        @Override
        public CompletionStage<Void> acquire() {
            CompletionStage<Void> stage = rateLimiter.acquire();
            cooldown = numCheckpointCooldown;
            return stage;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (cooldown-- <= 0) {
                rateLimiter.notifyCheckpointComplete(checkpointId);
            }
        }
    }

    private static class TrackingCommitter implements Committer<Record<Integer>>, Serializable {
        private final SharedReference<Queue<CommitRequest<Record<Integer>>>> committed;

        public TrackingCommitter(SharedReference<Queue<CommitRequest<Record<Integer>>>> committed) {
            this.committed = committed;
        }

        @Override
        public void commit(Collection<CommitRequest<Record<Integer>>> committables) {
            committed.get().addAll(committables);
        }

        @Override
        public void close() {}
    }
}
