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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A simple streaming program, which is using the state checkpointing of Flink. It is using a user
 * defined class as the state.
 */
@SuppressWarnings("serial")
public class CheckpointedStreamingProgram {

    private static final int CHECKPOINT_INTERVALL = 100;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(CHECKPOINT_INTERVALL);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 100L);
        env.disableOperatorChaining();

        DataStream<String> text =
                env.fromSource(
                        new SimpleStringGenerator(),
                        WatermarkStrategy.noWatermarks(),
                        "String Generator");
        text.map(new StatefulMapper()).sinkTo(new DiscardingSink<>());
        env.setParallelism(1);
        env.execute("Checkpointed Streaming Program");
    }

    private static class StatefulMapper
            implements MapFunction<String, String>,
                    ListCheckpointed<StatefulMapper>,
                    CheckpointListener {

        private String someState;
        private boolean atLeastOneSnapshotComplete = false;
        private boolean restored = false;

        @Override
        public List<StatefulMapper> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            return Collections.singletonList(this);
        }

        @Override
        public void restoreState(List<StatefulMapper> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            restored = true;
            StatefulMapper s = state.get(0);
            this.someState = s.someState;
            this.atLeastOneSnapshotComplete = s.atLeastOneSnapshotComplete;
        }

        @Override
        public String map(String value) throws Exception {
            if (!atLeastOneSnapshotComplete) {
                // throttle consumption by the checkpoint interval until we have one snapshot.
                Thread.sleep(CHECKPOINT_INTERVALL);
            }
            if (atLeastOneSnapshotComplete && !restored) {
                throw new RuntimeException("Intended failure, to trigger restore");
            }
            if (restored) {
                throw new SuccessException();
                // throw new RuntimeException("All good");
            }
            someState = value; // update our state
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            atLeastOneSnapshotComplete = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    private static class SimpleSplit implements SourceSplit {
        @Override
        public String splitId() {
            return "simple";
        }
    }

    private static class SimpleStringGenerator implements Source<String, SimpleSplit, Void> {
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SplitEnumerator<SimpleSplit, Void> createEnumerator(
                SplitEnumeratorContext<SimpleSplit> enumContext) throws Exception {
            return new SplitEnumerator<>() {
                private boolean assigned = false;

                @Override
                public void start() {}

                @Override
                public void handleSplitRequest(int subtaskId, String hostname) {
                    if (!assigned) {
                        enumContext.assignSplit(new SimpleSplit(), subtaskId);
                        assigned = true;
                    }
                }

                @Override
                public void addSplitsBack(List<SimpleSplit> splits, int subtaskId) {}

                @Override
                public void addReader(int subtaskId) {
                    if (!assigned) {
                        enumContext.assignSplit(new SimpleSplit(), subtaskId);
                        enumContext.signalNoMoreSplits(subtaskId);
                        assigned = true;
                    }
                }

                @Override
                public Void snapshotState(long checkpointId) {
                    return null;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public SplitEnumerator<SimpleSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<SimpleSplit> enumContext, Void checkpoint) throws Exception {
            return createEnumerator(enumContext);
        }

        @Override
        public SimpleVersionedSerializer<SimpleSplit> getSplitSerializer() {
            return new SimpleVersionedSerializer<>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(SimpleSplit split) {
                    return new byte[0];
                }

                @Override
                public SimpleSplit deserialize(int version, byte[] bytes) {
                    return new SimpleSplit();
                }
            };
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(Void obj) {
                    return new byte[0];
                }

                @Override
                public Void deserialize(int version, byte[] serialized) {
                    return null;
                }
            };
        }

        @Override
        public SourceReader<String, SimpleSplit> createReader(SourceReaderContext readerContext)
                throws Exception {
            return new SourceReader<>() {
                private volatile boolean running = true;
                private boolean splitReceived = false; // Add this

                @Override
                public void start() {}

                @Override
                public InputStatus pollNext(ReaderOutput<String> output) {
                    if (!running || !splitReceived) { // Wait for split
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return InputStatus.END_OF_INPUT;
                    }
                    output.collect("someString");
                    return InputStatus.MORE_AVAILABLE;
                }

                @Override
                public List<SimpleSplit> snapshotState(long checkpointId) {
                    // Return the current split if we have one
                    return splitReceived
                            ? Collections.singletonList(new SimpleSplit())
                            : Collections.emptyList();
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public void notifyNoMoreSplits() {
                    // Don't set running = false here
                }

                @Override
                public void addSplits(List<SimpleSplit> splits) {
                    if (!splits.isEmpty()) {
                        splitReceived = true; // Mark that we received a split
                    }
                }

                @Override
                public void close() {
                    running = false;
                }

                @Override
                public void notifyCheckpointComplete(long checkpointId) {}

                @Override
                public void notifyCheckpointAborted(long checkpointId) {}
            };
        }
    }

    private static class SimpleStringSplitEnumerator implements SplitEnumerator<SimpleSplit, Void> {
        private final SplitEnumeratorContext<SimpleSplit> context;
        private boolean splitAssigned = false;

        public SimpleStringSplitEnumerator(SplitEnumeratorContext<SimpleSplit> context) {
            this.context = context;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            if (!splitAssigned) {
                context.assignSplit(new SimpleSplit(), subtaskId);
                splitAssigned = true;
            }
        }

        @Override
        public void addSplitsBack(List<SimpleSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {
            if (!splitAssigned) {
                context.assignSplit(new SimpleSplit(), subtaskId);
                context.signalNoMoreSplits(subtaskId);
                splitAssigned = true;
            }
        }

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    /** We intentionally use a user specified failure exception. */
    private static class SuccessException extends Exception {}
}
