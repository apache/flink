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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.RichSourceReaderContext;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** A {@link Source} that reads data using an {@link InputFormat}. */
@Internal
public class InputFormatSource<OUT> implements Source<OUT, SourceSplit, Void> {
    private static final long serialVersionUID = 1L;

    private final Boundedness boundedness;
    private final InputFormat<OUT, InputSplit> format;

    @SuppressWarnings("unchecked")
    public InputFormatSource(Boundedness boundedness, InputFormat<OUT, ?> format) {
        this.boundedness = boundedness;
        this.format = (InputFormat<OUT, InputSplit>) format;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SplitEnumerator<SourceSplit, Void> createEnumerator(
            SplitEnumeratorContext<SourceSplit> context) throws Exception {
        return new InputFormatSplitEnumerator<>(format, context);
    }

    @Override
    public SplitEnumerator<SourceSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<SourceSplit> context, Void checkpoint) throws Exception {
        return new InputFormatSplitEnumerator<>(format, context);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(SourceSplit split) throws IOException {
                return InstantiationUtil.serializeObject(split);
            }

            @Override
            public SourceSplit deserialize(int version, byte[] serialized) throws IOException {
                try {
                    return InstantiationUtil.deserializeObject(
                            serialized, Thread.currentThread().getContextClassLoader());
                } catch (ClassNotFoundException e) {
                    throw new IOException("Failed to deserialize SourceSplit.", e);
                }
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 0;
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
    public SourceReader<OUT, SourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        RuntimeContext runtimeContext = null;
        if (readerContext instanceof RichSourceReaderContext) {
            runtimeContext = ((RichSourceReaderContext) readerContext).getRuntimeContext();
        }
        return new InputFormatSourceReader<>(readerContext, format, runtimeContext);
    }

    private static class InputSplitWrapperSourceSplit implements SourceSplit, Serializable {
        private final InputSplit inputSplit;
        private final String id;

        public InputSplitWrapperSourceSplit(InputSplit inputSplit) {
            this.inputSplit = inputSplit;
            this.id = String.valueOf(inputSplit.getSplitNumber());
        }

        public InputSplit getInputSplit() {
            return inputSplit;
        }

        @Override
        public String splitId() {
            return id;
        }
    }

    private static class InputFormatSplitEnumerator<OUT>
            implements SplitEnumerator<SourceSplit, Void> {
        private final InputFormat<OUT, InputSplit> format;
        private final SplitEnumeratorContext<SourceSplit> context;
        private Queue<SourceSplit> remainingSplits;

        public InputFormatSplitEnumerator(
                InputFormat<OUT, InputSplit> format, SplitEnumeratorContext<SourceSplit> context) {
            this.format = format;
            this.context = context;
        }

        @Override
        public void start() {
            try {
                remainingSplits =
                        Arrays.stream(format.createInputSplits(context.currentParallelism()))
                                .map(InputSplitWrapperSourceSplit::new)
                                .collect(Collectors.toCollection(LinkedList::new));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            final SourceSplit nextSplit = remainingSplits.poll();
            if (nextSplit != null) {
                context.assignSplit(nextSplit, subtaskId);
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<SourceSplit> splits, int subtaskId) {
            remainingSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {}
    }

    private static class InputFormatSourceReader<OUT> implements SourceReader<OUT, SourceSplit> {
        private final SourceReaderContext readerContext;
        private final InputFormat<OUT, InputSplit> format;
        @Nullable private final RuntimeContext runtimeContext;
        @Nullable private Counter completedSplitsCounter;
        private Queue<SourceSplit> remainingSplits;
        private boolean noMoreSplits;
        private boolean isFormatOpen;
        private OUT lastElement;

        public InputFormatSourceReader(
                SourceReaderContext readerContext,
                InputFormat<OUT, InputSplit> format,
                @Nullable RuntimeContext runtimeContext) {
            this.format = format;
            this.runtimeContext = runtimeContext;
            this.readerContext = readerContext;
        }

        @Override
        public void start() {
            this.remainingSplits = new LinkedList<>();
            if (runtimeContext != null) {
                completedSplitsCounter =
                        runtimeContext.getMetricGroup().counter("numSplitsProcessed");
            }
            this.noMoreSplits = false;
            this.isFormatOpen = false;
            this.lastElement = null;

            if (format instanceof RichInputFormat) {
                ((RichInputFormat<?, ?>) format).setRuntimeContext(runtimeContext);
            }
            format.configure(readerContext.getConfiguration());
            if (format instanceof RichInputFormat) {
                try {
                    ((RichInputFormat<?, ?>) format).openInputFormat();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            readerContext.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
            // If no open format then try to open it
            if (!isFormatOpen) {
                InputSplitWrapperSourceSplit split =
                        (InputSplitWrapperSourceSplit) remainingSplits.poll();
                if (split != null) {
                    format.open(split.getInputSplit());
                    isFormatOpen = true;

                    // We send a request for more
                    if (remainingSplits.isEmpty() && !noMoreSplits) {
                        readerContext.sendSplitRequest();
                    }
                }
            }

            // If there is a format which is not at the end then return the next record
            if (isFormatOpen && !format.reachedEnd()) {
                lastElement = format.nextRecord(lastElement);
                output.collect(lastElement);
                return InputStatus.MORE_AVAILABLE;
            } else {
                // Otherwise just close it
                format.close();
                isFormatOpen = false;
                if (completedSplitsCounter != null) {
                    completedSplitsCounter.inc();
                }
            }

            // Here we have nothing to collect
            if (remainingSplits.isEmpty()) {
                if (noMoreSplits) {
                    // No further data so signal end
                    return InputStatus.END_OF_INPUT;
                } else {
                    // When there are splits remote then we signal nothing available
                    return InputStatus.NOTHING_AVAILABLE;
                }
            } else {
                // When we have splits locally then we just need to process them in the next
                // round
                return InputStatus.MORE_AVAILABLE;
            }
        }

        @Override
        public List<SourceSplit> snapshotState(long checkpointId) {
            return List.of();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<SourceSplit> splits) {
            remainingSplits.addAll(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void close() throws Exception {
            if (isFormatOpen) {
                format.close();
                isFormatOpen = false;
            }
            if (format instanceof RichInputFormat) {
                ((RichInputFormat<?, ?>) format).closeInputFormat();
            }
        }
    }
}
