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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.watermark.InternalWatermarkDeclaration;
import org.apache.flink.runtime.watermark.ReusableWatermarkContext;
import org.apache.flink.runtime.watermark.WatermarkOutput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
public abstract class AbstractStreamTaskNetworkInput<
                T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>>
        implements StreamTaskInput<T> {
    protected final CheckpointedInputGate checkpointedInputGate;
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;
    protected final TypeSerializer<T> inputSerializer;
    protected final Map<InputChannelInfo, R> recordDeserializers;
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();
    /** Valve that controls how watermarks and watermark statuses are forwarded. */
    protected final StatusWatermarkValve statusWatermarkValve;

    protected final int inputIndex;
    private final RecordAttributesCombiner recordAttributesCombiner;
    private InputChannelInfo lastChannel = null;
    private R currentRecordDeserializer = null;
    private final ReusableWatermarkContext reusableWatermarkContext;
    protected final Map<String, InternalWatermarkDeclaration.WatermarkCombiner> watermarkCombiners =
            new HashMap<>();

    protected final CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        this(
                checkpointedInputGate,
                inputSerializer,
                statusWatermarkValve,
                inputIndex,
                recordDeserializers,
                canEmitBatchOfRecords,
                new HashSet<>());
    }

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords,
            Set<InternalWatermarkDeclaration> watermarkDeclarationSet) {
        super();
        this.checkpointedInputGate = checkpointedInputGate;
        List<InternalWatermarkDeclaration.WatermarkSerde> watermarkDeclarations =
                watermarkDeclarationSet.stream()
                        .map(w -> w.declaredWatermark())
                        .collect(Collectors.toList());
        deserializationDelegate =
                new NonReusingDeserializationDelegate<>(
                        new StreamElementSerializer<>(inputSerializer, watermarkDeclarations));
        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
        this.canEmitBatchOfRecords = checkNotNull(canEmitBatchOfRecords);
        this.recordAttributesCombiner =
                new RecordAttributesCombiner(checkpointedInputGate.getNumberOfInputChannels());
        this.reusableWatermarkContext =
                new ReusableWatermarkContext(flattenedChannelIndices.size(), 0);

        for (InternalWatermarkDeclaration watermarkDeclaration : watermarkDeclarationSet) {
            watermarkCombiners.put(
                    watermarkDeclaration.getIdentifier(), watermarkDeclaration.watermarkCombiner());
        }
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

        while (true) {
            // get the stream element from the deserializer
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                } catch (IOException e) {
                    throw new IOException(
                            String.format("Can't get next record for channel %s", lastChannel), e);
                }
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) {
                    final boolean breakBatchEmitting =
                            processElement(deserializationDelegate.getInstance(), output);
                    if (canEmitBatchOfRecords.check() && !breakBatchEmitting) {
                        continue;
                    }
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }

            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                if (bufferOrEvent.get().isBuffer()) {
                    processBuffer(bufferOrEvent.get());
                } else {
                    DataInputStatus status = processEvent(bufferOrEvent.get());
                    if (status == DataInputStatus.MORE_AVAILABLE && canEmitBatchOfRecords.check()) {
                        continue;
                    }
                    return status;
                }
            } else {
                if (checkpointedInputGate.isFinished()) {
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    return DataInputStatus.END_OF_INPUT;
                }
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    /**
     * Process the given stream element and returns whether to stop processing and return from the
     * emitNext method so that the emitNext is invoked again right after processing the element to
     * allow behavior change in emitNext method. For example, the behavior of emitNext may need to
     * change right after process a RecordAttributes.
     */
    private boolean processElement(StreamElement streamElement, DataOutput<T> output)
            throws Exception {
        if (streamElement.isRecord()) {
            output.emitRecord(streamElement.asRecord());
            return false;
        } else if (streamElement.isWatermark()) {
            statusWatermarkValve.inputWatermark(
                    streamElement.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
            return false;
        } else if (streamElement.isLatencyMarker()) {
            output.emitLatencyMarker(streamElement.asLatencyMarker());
            return false;
        } else if (streamElement.isWatermarkStatus()) {
            statusWatermarkValve.inputWatermarkStatus(
                    streamElement.asWatermarkStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
            return false;
        } else if (streamElement.isRecordAttributes()) {
            recordAttributesCombiner.inputRecordAttributes(
                    streamElement.asRecordAttributes(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
            return true;
        } else if (streamElement.isGeneralizedWatermark()) {
            GeneralizedWatermark genericWatermark =
                    streamElement.asGeneralizedWatermark().getWatermark();

            WatermarkOutput watermarkOutput =
                    w -> {
                        try {
                            output.emitGeneralizedWatermark(new GeneralizedWatermarkEvent(w));
                        } catch (Exception e) {
                            throw new FlinkRuntimeException(e);
                        }
                    };

            reusableWatermarkContext.setChannelInfo(
                    flattenedChannelIndices.size(), lastChannel.getInputChannelIdx());
            watermarkCombiners.compute(
                    genericWatermark.getIdentifier(),
                    (key, watermarkCombiner) -> {
                        if (watermarkCombiner != null) {
                            try {
                                watermarkCombiner.combineWatermark(
                                        genericWatermark,
                                        reusableWatermarkContext,
                                        watermarkOutput);
                            } catch (Exception e) {
                                throw new FlinkRuntimeException(e);
                            }
                            return watermarkCombiner; // Return the same combiner to maintain the
                            // map entry
                        } else {
                            throw new FlinkRuntimeException(
                                    "Unknown watermark identifier "
                                            + genericWatermark.getIdentifier());
                        }
                    });
            return true;
        } else {
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }

    protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        if (event.getClass() == EndOfData.class) {
            switch (checkpointedInputGate.hasReceivedEndOfData()) {
                case NOT_END_OF_DATA:
                    // skip
                    break;
                case DRAINED:
                    return DataInputStatus.END_OF_DATA;
                case STOPPED:
                    return DataInputStatus.STOPPED;
            }
        } else if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
            if (checkpointedInputGate.isFinished()) {
                return DataInputStatus.END_OF_INPUT;
            }
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return DataInputStatus.END_OF_RECOVERY;
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);
        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(
                currentRecordDeserializer != null,
                "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
