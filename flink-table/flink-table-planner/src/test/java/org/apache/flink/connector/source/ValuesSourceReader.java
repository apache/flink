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
import org.apache.flink.connector.source.split.ValuesSourceSplit;
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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/** A {@link SourceReader} implementation that reads data from a list. */
public class ValuesSourceReader implements SourceReader<RowData, ValuesSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(ValuesSourceReader.class);

    /** The context for this reader, to communicate with the enumerator. */
    private final SourceReaderContext context;

    /** The availability future. This reader is available as soon as a split is assigned. */
    private CompletableFuture<Void> availability;

    private final List<byte[]> serializedElements;
    private final TypeSerializer<RowData> serializer;
    private List<RowData> elements;

    /** The remaining splits that were assigned but not yet processed. */
    private final Queue<ValuesSourceSplit> remainingSplits;

    private boolean noMoreSplits;

    public ValuesSourceReader(
            List<byte[]> serializedElements,
            TypeSerializer<RowData> serializer,
            SourceReaderContext context) {
        this.serializedElements = serializedElements;
        this.serializer = serializer;
        this.context = context;
        this.availability = new CompletableFuture<>();
        this.remainingSplits = new ArrayDeque<>();
    }

    @Override
    public void start() {
        elements = new ArrayList<>();
        for (byte[] bytes : serializedElements) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
                DataInputView input = new DataInputViewStreamWrapper(bais);
                RowData element = serializer.deserialize(input);
                elements.add(element);
            } catch (Exception e) {
                throw new TableException(
                        "Failed to deserialize an element from the source. "
                                + "If you are using user-defined serialization (Value and Writable types), check the "
                                + "serialization functions.\nSerializer is "
                                + serializer,
                        e);
            }
        }
        // request a split if we don't have one
        if (remainingSplits.isEmpty()) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        ValuesSourceSplit currentSplit = remainingSplits.poll();
        if (currentSplit != null) {
            if (currentSplit.isInfinite()) {
                remainingSplits.add(currentSplit);
                resetAvailability();
                return InputStatus.NOTHING_AVAILABLE;
            } else {
                output.collect(elements.get(currentSplit.getIndex()));
                // request another split
                context.sendSplitRequest();
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

    @Override
    public List<ValuesSourceSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<ValuesSourceSplit> splits) {
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
