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

package org.apache.flink.connector.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.source.split.ValuesSourcePartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link SourceReader} implementation that reads data from a list. */
public class DynamicFilteringValuesSourceReader
        implements SourceReader<RowData, ValuesSourcePartitionSplit> {
    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicFilteringValuesSourceReader.class);

    /** The context for this reader, to communicate with the enumerator. */
    private final SourceReaderContext context;

    /** The availability future. This reader is available as soon as a split is assigned. */
    private CompletableFuture<Void> availability;

    private final TypeSerializer<RowData> serializer;
    private final Map<Map<String, String>, byte[]> serializedElements;
    private final Map<Map<String, String>, Integer> counts;
    private final Queue<ValuesSourcePartitionSplit> remainingSplits;

    private transient ValuesSourcePartitionSplit currentSplit;
    private transient Iterator<RowData> iterator;
    private transient boolean noMoreSplits;
    private transient boolean reachedInfiniteEnd;

    public DynamicFilteringValuesSourceReader(
            Map<Map<String, String>, byte[]> serializedElements,
            Map<Map<String, String>, Integer> counts,
            TypeSerializer<RowData> serializer,
            SourceReaderContext context) {
        this.serializedElements = checkNotNull(serializedElements);
        this.counts = checkNotNull(counts);
        this.serializer = serializer;
        this.context = checkNotNull(context);
        this.availability = new CompletableFuture<>();
        this.remainingSplits = new ArrayDeque<>();
    }

    @Override
    public void start() {
        // request a split if we don't have one
        if (remainingSplits.isEmpty()) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) {
        if (reachedInfiniteEnd) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (iterator != null) {
            if (iterator.hasNext()) {
                output.collect(iterator.next());
                return InputStatus.MORE_AVAILABLE;
            } else {
                finishSplit();
            }
        }

        return tryMoveToNextSplit();
    }

    private void finishSplit() {
        iterator = null;
        currentSplit = null;

        // request another split if no other is left
        // we do this only here in the finishSplit part to avoid requesting a split
        // whenever the reader is polled and doesn't currently have a split
        if (remainingSplits.isEmpty() && !noMoreSplits) {
            context.sendSplitRequest();
        }
    }

    private InputStatus tryMoveToNextSplit() {
        currentSplit = remainingSplits.poll();
        if (currentSplit != null) {
            if (currentSplit.isInfinite()) {
                this.reachedInfiniteEnd = true;
                resetAvailability();
                return InputStatus.NOTHING_AVAILABLE;
            } else {
                Map<String, String> partition = currentSplit.getPartition();
                List<RowData> list =
                        deserialize(serializedElements.get(partition), counts.get(partition));
                iterator = list.iterator();
                return InputStatus.MORE_AVAILABLE;
            }
        } else if (noMoreSplits) {
            return InputStatus.END_OF_INPUT;
        } else {
            resetAvailability();
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    private void resetAvailability() {
        // ensure we are not called in a loop by resetting the availability future
        if (availability.isDone()) {
            availability = new CompletableFuture<>();
        }
    }

    private List<RowData> deserialize(byte[] data, int count) {
        List<RowData> list = new ArrayList<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
            final DataInputView input = new DataInputViewStreamWrapper(bais);
            for (int i = 0; i < count; ++i) {
                RowData element = serializer.deserialize(input);
                list.add(element);
            }
        } catch (Exception e) {
            throw new TableException(
                    "Failed to deserialize an element from the source. "
                            + "If you are using user-defined serialization (Value and Writable types), check the "
                            + "serialization functions.\nSerializer is "
                            + serializer,
                    e);
        }
        return list;
    }

    @Override
    public List<ValuesSourcePartitionSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<ValuesSourcePartitionSplit> splits) {
        remainingSplits.addAll(splits);
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.info("checkpoint {} finished.", checkpointId);
    }
}
