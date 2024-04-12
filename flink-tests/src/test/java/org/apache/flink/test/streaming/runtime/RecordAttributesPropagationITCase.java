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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link RecordAttributes} propagation. */
public class RecordAttributesPropagationITCase {

    @Test
    void testRecordAttributesPropagation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final SourceWithBacklog source1 = new SourceWithBacklog();
        final SourceWithBacklog source2 = new SourceWithBacklog();

        env.fromSource(source1, WatermarkStrategy.noWatermarks(), "source1")
                .returns(Long.class)
                .transform("my_op1", Types.LONG, new OneInputOperator())
                .connect(
                        env.fromSource(source2, WatermarkStrategy.noWatermarks(), "source2")
                                .returns(Long.class))
                .transform("my_op2", Types.LONG, new TwoInputOperator())
                .addSink(new DiscardingSink<>());
        env.execute();
        final RecordAttributes backlog =
                new RecordAttributesBuilder(Collections.emptyList()).setBacklog(true).build();
        final RecordAttributes nonBacklog =
                new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build();
        assertThat(OneInputOperator.receivedRecordAttributes).containsExactly(backlog, nonBacklog);
        assertThat(TwoInputOperator.receivedRecordAttributes1).containsExactly(backlog, nonBacklog);
        assertThat(TwoInputOperator.receivedRecordAttributes2).containsExactly(backlog, nonBacklog);
    }

    static class SourceWithBacklog implements Source<Long, MockSplit, Long> {

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SplitEnumerator<MockSplit, Long> createEnumerator(
                SplitEnumeratorContext<MockSplit> enumContext) {
            return new SplitEnumeratorWithBacklog(enumContext);
        }

        @Override
        public SplitEnumerator<MockSplit, Long> restoreEnumerator(
                SplitEnumeratorContext<MockSplit> enumContext, Long checkpoint) {
            return new SplitEnumeratorWithBacklog(enumContext);
        }

        @Override
        public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
            return new SimpleVersionedSerializer<MockSplit>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(MockSplit obj) {
                    return new byte[0];
                }

                @Override
                public MockSplit deserialize(int version, byte[] serialized) {
                    return new MockSplit();
                }
            };
        }

        @Override
        public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<Long>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(Long obj) {
                    return new byte[0];
                }

                @Override
                public Long deserialize(int version, byte[] serialized) {
                    return 0L;
                }
            };
        }

        @Override
        public SourceReader<Long, MockSplit> createReader(SourceReaderContext readerContext) {
            return new SourceReader<Long, MockSplit>() {
                private boolean noMoreSplit;

                @Override
                public void start() {}

                @Override
                public InputStatus pollNext(ReaderOutput<Long> output) {
                    if (noMoreSplit) {
                        return InputStatus.END_OF_INPUT;
                    }
                    return InputStatus.MORE_AVAILABLE;
                }

                @Override
                public List<MockSplit> snapshotState(long checkpointId) {
                    return null;
                }

                @Override
                public CompletableFuture<Void> isAvailable() {
                    return null;
                }

                @Override
                public void addSplits(List<MockSplit> splits) {}

                @Override
                public void notifyNoMoreSplits() {
                    noMoreSplit = true;
                }

                @Override
                public void close() {}
            };
        }
    }

    static class SplitEnumeratorWithBacklog implements SplitEnumerator<MockSplit, Long> {

        private final SplitEnumeratorContext<MockSplit> context;
        private final ExecutorService executor;

        SplitEnumeratorWithBacklog(SplitEnumeratorContext<MockSplit> context) {
            this.context = context;
            executor = Executors.newSingleThreadExecutor();
        }

        @Override
        public void start() {
            executor.submit(
                    () -> {
                        context.setIsProcessingBacklog(true);
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        context.setIsProcessingBacklog(false);
                        for (int i = 0; i < context.currentParallelism(); i++) {
                            context.signalNoMoreSplits(i);
                        }
                    });
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

        @Override
        public void addSplitsBack(List<MockSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Long snapshotState(long checkpointId) throws Exception {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    static class MockSplit implements SourceSplit, Serializable {
        @Override
        public String splitId() {
            return "0";
        }
    }

    static class OneInputOperator extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Long, Long> {
        private static final List<RecordAttributes> receivedRecordAttributes = new ArrayList<>();

        @Override
        public void processElement(StreamRecord<Long> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            receivedRecordAttributes.add(recordAttributes);
            super.processRecordAttributes(recordAttributes);
        }
    }

    static class TwoInputOperator extends AbstractStreamOperator<Long>
            implements TwoInputStreamOperator<Long, Long, Long> {

        private static final List<RecordAttributes> receivedRecordAttributes1 = new ArrayList<>();
        private static final List<RecordAttributes> receivedRecordAttributes2 = new ArrayList<>();

        @Override
        public void processRecordAttributes1(RecordAttributes recordAttributes) {
            receivedRecordAttributes1.add(recordAttributes);
            super.processRecordAttributes1(recordAttributes);
        }

        @Override
        public void processRecordAttributes2(RecordAttributes recordAttributes) {
            receivedRecordAttributes2.add(recordAttributes);
            super.processRecordAttributes2(recordAttributes);
        }

        @Override
        public void processElement1(StreamRecord<Long> element) {
            output.collect(element);
        }

        @Override
        public void processElement2(StreamRecord<Long> element) {
            output.collect(element);
        }
    }
}
