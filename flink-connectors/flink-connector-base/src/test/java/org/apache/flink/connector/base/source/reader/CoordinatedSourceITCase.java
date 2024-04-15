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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitEnumerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for the {@link Source} with a coordinator. */
public class CoordinatedSourceITCase extends AbstractTestBase {

    @Test
    public void testEnumeratorReaderCommunication() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockBaseSource source = new MockBaseSource(2, 10, Boundedness.BOUNDED);
        DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @Test
    public void testMultipleSources() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockBaseSource source1 = new MockBaseSource(2, 10, Boundedness.BOUNDED);
        MockBaseSource source2 = new MockBaseSource(2, 10, 20, Boundedness.BOUNDED);
        DataStream<Integer> stream1 =
                env.fromSource(source1, WatermarkStrategy.noWatermarks(), "TestingSource1");
        DataStream<Integer> stream2 =
                env.fromSource(source2, WatermarkStrategy.noWatermarks(), "TestingSource2");
        executeAndVerify(env, stream1.union(stream2), 40);
    }

    @Test
    public void testEnumeratorCreationFails() throws Exception {
        OnceFailingToCreateEnumeratorSource.reset();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        final Source<Integer, ?, ?> source =
                new OnceFailingToCreateEnumeratorSource(2, 10, Boundedness.BOUNDED);
        final DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @Test
    public void testEnumeratorRestoreFails() throws Exception {
        OnceFailingToRestoreEnumeratorSource.reset();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        env.enableCheckpointing(10);

        final Source<Integer, ?, ?> source =
                new OnceFailingToRestoreEnumeratorSource(2, 10, Boundedness.BOUNDED);
        final DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @SuppressWarnings("serial")
    private void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<Integer> stream, int numRecords)
            throws Exception {
        stream.addSink(
                new RichSinkFunction<Integer>() {
                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<Integer>());
                    }

                    @Override
                    public void invoke(Integer value, Context context) throws Exception {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });
        List<Integer> result = env.execute().getAccumulatorResult("result");
        Collections.sort(result);
        assertThat(result).hasSize(numRecords);
        assertThat((int) result.get(0)).isEqualTo(0);
        assertThat((int) result.get(result.size() - 1)).isEqualTo(numRecords - 1);
    }

    // ------------------------------------------------------------------------

    private static class OnceFailingToCreateEnumeratorSource extends MockBaseSource {

        private static final long serialVersionUID = 1L;
        private static boolean hasFailed;

        OnceFailingToCreateEnumeratorSource(
                int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
            super(numSplits, numRecordsPerSplit, boundedness);
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {
            if (!hasFailed) {
                hasFailed = true;
                throw new FlinkRuntimeException("Test Failure");
            }

            return super.createEnumerator(enumContext);
        }

        static void reset() {
            hasFailed = false;
        }
    }

    /**
     * A source with the following behavior:
     *
     * <ol>
     *   <li>It initially creates an enumerator that does not assign work, waits until the first
     *       checkpoint completes (which contains all work, because none is assigned, yet) and then
     *       triggers a global failure.
     *   <li>Upon restoring from the failure, the first attempt to restore the enumerator fails with
     *       an exception.
     *   <li>The next time to restore the enumerator succeeds and the enumerator works regularly.
     * </ol>
     */
    private static class OnceFailingToRestoreEnumeratorSource extends MockBaseSource {

        private static final long serialVersionUID = 1L;
        private static boolean hasFailed;

        OnceFailingToRestoreEnumeratorSource(
                int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
            super(numSplits, numRecordsPerSplit, boundedness);
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {

            final SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> enumerator =
                    super.createEnumerator(enumContext);

            if (hasFailed) {
                // after the failure happened, we proceed normally
                return enumerator;
            } else {
                // before the failure, we go with
                try {
                    final List<MockSourceSplit> splits = enumerator.snapshotState(1L);
                    return new NonAssigningEnumerator(splits, enumContext);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(e.getMessage(), e);
                }
            }
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> restoreEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext,
                List<MockSourceSplit> checkpoint)
                throws IOException {
            if (!hasFailed) {
                hasFailed = true;
                throw new FlinkRuntimeException("Test Failure");
            }

            return super.restoreEnumerator(enumContext, checkpoint);
        }

        static void reset() {
            hasFailed = false;
        }

        /**
         * This enumerator does not assign work, so all state is in the checkpoint. After the first
         * checkpoint is complete, it triggers a global failure.
         */
        private static class NonAssigningEnumerator extends MockSplitEnumerator {

            private final SplitEnumeratorContext<?> context;

            NonAssigningEnumerator(
                    List<MockSourceSplit> splits, SplitEnumeratorContext<MockSourceSplit> context) {
                super(splits, context);
                this.context = context;
            }

            @Override
            public void addReader(int subtaskId) {
                // we do nothing here to make sure there is no progress
            }

            @Override
            public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
                // we do nothing here to make sure there is no progress
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) throws Exception {
                // This is a bit of a clumsy way to trigger a global failover from a coordinator.
                // This is safe, though, because per the contract, exceptions in the enumerator
                // handlers trigger a global failover.
                context.callAsync(
                        () -> null,
                        (success, failure) -> {
                            throw new FlinkRuntimeException(
                                    "Artificial trigger for Global Failover");
                        });
            }
        }
    }
}
