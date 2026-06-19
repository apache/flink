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

package org.apache.flink.test.util.source;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests showing how to create sources using AbstractTestSource utilities for common patterns. */
class SourcePatternExamplesTest {

    /*
     * Testing: Source that must call external service on each data emission
     * Why: Common pattern where legacy sources coordinate with external systems during processing
     * Pattern: Business logic goes in pollNext(), everything else uses defaults from AbstractTestSource
     */
    @Test
    void testSourceThatInteractsWithProcessingTimeService() throws Exception {
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        List<Long> processingTimes = Arrays.asList(1000L, 2000L);
        AbstractTestSource<Long> source =
                new AbstractTestSource<Long>() {
                    @Override
                    public SourceReader<Long, TestSplit> createReader(
                            SourceReaderContext readerContext) {
                        return new TestSourceReader<Long>(readerContext) {
                            private int currentIndex = 0;
                            private volatile boolean cancelled = false;

                            @Override
                            public InputStatus pollNext(ReaderOutput<Long> output) {
                                if (cancelled || currentIndex >= processingTimes.size()) {
                                    return InputStatus.END_OF_INPUT;
                                }
                                Long processingTime = processingTimes.get(currentIndex++);
                                processingTimeService.setCurrentTime(processingTime);
                                output.collect(processingTime);
                                return currentIndex >= processingTimes.size()
                                        ? InputStatus.END_OF_INPUT
                                        : InputStatus.MORE_AVAILABLE;
                            }

                            @Override
                            public void close() throws Exception {
                                cancelled = true;
                                super.close();
                            }
                        };
                    }
                };

        // Test the coordination pattern: verify each poll calls external service + emits data
        SourceReader<Long, TestSplit> reader = source.createReader(null);
        TestReaderOutput<Long> output = new TestReaderOutput<>();

        // First poll: time advances to 1000L and emits it
        reader.pollNext(output);
        assertThat(output.getLastEmitted()).isEqualTo(1000L);
        assertThat(processingTimeService.getCurrentTime()).isEqualTo(1000L);

        // Second poll: time advances to 2000L and emits it
        reader.pollNext(output);
        assertThat(output.getLastEmitted()).isEqualTo(2000L);
        assertThat(processingTimeService.getCurrentTime()).isEqualTo(2000L);

        // Third poll: no more data, should signal end
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
    }

    /*
     * Testing: Source that generates infinite data stream without meaningful checkpointing
     * Why: Common pattern for continuous data generation that doesn't actually checkpoint state
     * Pattern: Use AbstractTestSource for unbounded sources that don't need real checkpointing
     */
    @Test
    void testInfiniteStringGeneratorWithoutCheckpointing() throws Exception {
        AbstractTestSource<String> source =
                new AbstractTestSource<>() {

                    @Override
                    public SourceReader<String, TestSplit> createReader(
                            SourceReaderContext readerContext) {
                        return new TestSourceReader<>(readerContext) {
                            private volatile boolean running = true;
                            private int emissionCount = 0;

                            @Override
                            public InputStatus pollNext(ReaderOutput<String> output)
                                    throws Exception {
                                if (!running) {
                                    return InputStatus.END_OF_INPUT;
                                }
                                output.collect("someString");
                                emissionCount++;
                                // Stop after 3 emissions to avoid infinite test
                                if (emissionCount >= 3) {
                                    running = false;
                                    return InputStatus.END_OF_INPUT;
                                }

                                return InputStatus.MORE_AVAILABLE;
                            }

                            @Override
                            public void close() throws Exception {
                                running = false;
                                super.close();
                            }
                        };
                    }
                };

        // Test data generation pattern
        SourceReader<String, TestSplit> reader = source.createReader(null);
        TestReaderOutput<String> output = new TestReaderOutput<>();

        // First poll: generates "someString"
        reader.pollNext(output);
        assertThat(output.getLastEmitted()).isEqualTo("someString");

        // Second poll: generates another "someString"
        reader.pollNext(output);
        assertThat(output.getLastEmitted()).isEqualTo("someString");
        assertThat(output.getCollectedCount()).isEqualTo(2);

        // Third poll: generates final "someString" and signals end
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollectedCount()).isEqualTo(3);
    }

    /*
     * Testing: Source that blocks using Thread.sleep() until external signal
     * Why: Some sources legitimately need blocking behavior in pollNext()
     * Pattern: Keep Thread.sleep() in pollNext() when blocking is actually required
     */
    @Test
    void testSourceWithLegitimateBlocking() throws Exception {
        final boolean[] shouldCloseSource = {false};

        AbstractTestSource<String> source =
                new AbstractTestSource<>() {

                    @Override
                    public SourceReader<String, TestSplit> createReader(
                            SourceReaderContext readerContext) {
                        return new TestSourceReader<String>(readerContext) {
                            private int pollCount = 0;

                            @Override
                            public InputStatus pollNext(ReaderOutput<String> output)
                                    throws Exception {
                                pollCount++;

                                if (shouldCloseSource[0]) {
                                    return InputStatus.END_OF_INPUT;
                                }
                                Thread.sleep(100);
                                if (pollCount >= 2) {
                                    shouldCloseSource[0] = true;
                                    return InputStatus.END_OF_INPUT;
                                }

                                return InputStatus.NOTHING_AVAILABLE;
                            }

                            @Override
                            public void close() throws Exception {
                                shouldCloseSource[0] = true;
                                super.close();
                            }
                        };
                    }
                };

        // Test blocking behavior (but limit to avoid infinite test)
        SourceReader<String, TestSplit> reader = source.createReader(null);
        TestReaderOutput<String> output = new TestReaderOutput<>();
        reader.pollNext(output);
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollectedCount()).isEqualTo(0);
    }

    /*
     * Testing: Source that emits timestamped data and watermarks
     * Why: Common pattern for event-time processing with explicit timestamp assignment
     * Pattern: Use collect(record, timestamp) and emitWatermark() in pollNext()
     */
    @Test
    void testSourceWithTimestampsAndWatermarks() throws Exception {
        AbstractTestSource<Integer> source =
                new AbstractTestSource<Integer>() {
                    @Override
                    public SourceReader<Integer, TestSplit> createReader(
                            SourceReaderContext readerContext) {
                        return new TestSourceReader<Integer>(readerContext) {
                            private int step = 0;

                            @Override
                            public InputStatus pollNext(ReaderOutput<Integer> output)
                                    throws Exception {
                                switch (step++) {
                                    case 0:
                                        // Emit record with timestamp
                                        output.collect(1, 0);
                                        return InputStatus.MORE_AVAILABLE;
                                    case 1:
                                        // Emit watermark
                                        output.emitWatermark(new Watermark(0));
                                        return InputStatus.MORE_AVAILABLE;
                                    case 2:
                                        output.collect(2, 1);
                                        return InputStatus.END_OF_INPUT;
                                    default:
                                        return InputStatus.END_OF_INPUT;
                                }
                            }
                        };
                    }
                };

        // Verify source handles timestamped data and watermarks
        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        // Test timestamp and watermark emission pattern
        SourceReader<Integer, TestSplit> reader = source.createReader(null);
        TestReaderOutput<Integer> output = new TestReaderOutput<>();

        // Step through the sequence of emissions
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE); // collect(1, 0)
        assertThat(output.getCollectedCount()).isEqualTo(1);
        assertThat(output.getLastEmitted()).isEqualTo(1);

        assertThat(reader.pollNext(output))
                .isEqualTo(InputStatus.MORE_AVAILABLE); // emitWatermark(0)
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT); // collect(2, 1)
        assertThat(output.getCollectedCount()).isEqualTo(2);
        assertThat(output.getLastEmitted()).isEqualTo(2);
    }

    /*
     * Testing: Source with custom enumerator checkpointing that AbstractTestSource cannot handle
     * Why: Demonstrates how to use AbstractTestSourceBase when you need real checkpoint state
     * Pattern: Custom checkpoint type (Integer) with stateful enumerator that tracks emission count
     */
    @Test
    void testSourceWithCustomEnumeratorCheckpointing() throws Exception {
        AbstractTestSourceBase<String, Integer> source =
                new AbstractTestSourceBase<>() {
                    @Override
                    public SourceReader<String, TestSplit> createReader(
                            SourceReaderContext readerContext) {
                        return new TestSourceReader<>(readerContext) {
                            private int emissionCount = 0;

                            @Override
                            public InputStatus pollNext(ReaderOutput<String> output) {
                                if (emissionCount >= 3) {
                                    return InputStatus.END_OF_INPUT;
                                }
                                output.collect("data-" + emissionCount);
                                emissionCount++;
                                return emissionCount >= 3
                                        ? InputStatus.END_OF_INPUT
                                        : InputStatus.MORE_AVAILABLE;
                            }
                        };
                    }

                    @Override
                    public SplitEnumerator<TestSplit, Integer> createEnumerator(
                            SplitEnumeratorContext<TestSplit> enumContext) {
                        return new TestSplitEnumerator<Integer>(enumContext, null) {
                            @Override
                            public Integer snapshotState(long checkpointId) {
                                return 1;
                            }
                        };
                    }

                    @Override
                    public SplitEnumerator<TestSplit, Integer> restoreEnumerator(
                            SplitEnumeratorContext<TestSplit> enumContext, Integer checkpoint) {
                        return new TestSplitEnumerator<Integer>(enumContext, checkpoint) {
                            @Override
                            public Integer snapshotState(long checkpointId) {
                                return checkpoint != null ? checkpoint + 1 : 1;
                            }
                        };
                    }

                    @Override
                    public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
                        return new SimpleVersionedSerializer<Integer>() {
                            @Override
                            public int getVersion() {
                                return 1;
                            }

                            @Override
                            public byte[] serialize(Integer obj) throws IOException {
                                return obj != null ? obj.toString().getBytes() : new byte[0];
                            }

                            @Override
                            public Integer deserialize(int version, byte[] serialized) {
                                return serialized.length > 0
                                        ? Integer.parseInt(new String(serialized))
                                        : 0;
                            }
                        };
                    }
                };

        // Test source reader functionality
        SourceReader<String, TestSplit> reader = source.createReader(null);
        TestReaderOutput<String> output = new TestReaderOutput<>();

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(output.getLastEmitted()).isEqualTo("data-0");

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(output.getLastEmitted()).isEqualTo("data-1");

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getLastEmitted()).isEqualTo("data-2");
        assertThat(output.getCollectedCount()).isEqualTo(3);
    }

    /*
     * Testing: No-op source that requires no reader implementation
     * Why: Some tests just need a source for framework testing without data emission
     * Pattern: Use AbstractTestSourceBase/AbstractTestSource directly without overriding createReader
     */
    @Test
    void testNoopSourceWithDefaultReader() throws Exception {
        // Create a no-op source with Void checkpointing (most common case)
        AbstractTestSource<String> noopSource = new AbstractTestSource<>() {};

        // Test that it works without any reader implementation
        SourceReader<String, TestSplit> reader = noopSource.createReader(null);
        TestReaderOutput<String> output = new TestReaderOutput<>();

        // Should immediately return END_OF_INPUT without emitting anything
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollectedCount()).isEqualTo(0);
        assertThat(output.getLastEmitted()).isNull();

        // Verify source properties
        assertThat(noopSource.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    /*
     * Testing: No-op source with custom checkpoint state but no data emission
     * Why: Testing enumerator checkpointing behavior without reader complexity
     * Pattern: Use AbstractTestSourceBase with custom state, rely on default empty reader
     */
    @Test
    void testNoopSourceWithCustomCheckpointing() throws Exception {
        AbstractTestSourceBase<Integer, String> noopSourceWithCheckpoint =
                new AbstractTestSourceBase<Integer, String>() {
                    @Override
                    public SimpleVersionedSerializer<String> getEnumeratorCheckpointSerializer() {
                        return new SimpleVersionedSerializer<String>() {
                            @Override
                            public int getVersion() {
                                return 1;
                            }

                            @Override
                            public byte[] serialize(String obj) {
                                return obj != null ? obj.getBytes() : new byte[0];
                            }

                            @Override
                            public String deserialize(int version, byte[] serialized) {
                                return serialized.length > 0 ? new String(serialized) : "";
                            }
                        };
                    }
                };

        // Test that reader works without implementation
        SourceReader<Integer, TestSplit> reader = noopSourceWithCheckpoint.createReader(null);
        TestReaderOutput<Integer> output = new TestReaderOutput<>();

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollectedCount()).isEqualTo(0);

        // Test enumerator checkpoint serialization
        SimpleVersionedSerializer<String> serializer =
                noopSourceWithCheckpoint.getEnumeratorCheckpointSerializer();
        byte[] data = serializer.serialize("test-checkpoint");
        String restored = serializer.deserialize(serializer.getVersion(), data);
        assertThat(restored).isEqualTo("test-checkpoint");
    }

    // Helper classes for testing
    private static class TestProcessingTimeService {
        private long currentTime = 0;

        public void setCurrentTime(long time) {
            this.currentTime = time;
        }

        public long getCurrentTime() {
            return currentTime;
        }
    }
}
