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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

final class CollectionDataInput<E> implements StreamTaskInput<E> {
    private final Iterator<StreamElement> elementsIterator;
    private final int inputIdx;

    CollectionDataInput(Collection<StreamElement> elements) {
        this(elements, 0);
    }

    CollectionDataInput(Collection<StreamElement> elements, int inputIdx) {
        this.elementsIterator = elements.iterator();
        this.inputIdx = inputIdx;
    }

    @Override
    public InputStatus emitNext(DataOutput<E> output) throws Exception {
        if (elementsIterator.hasNext()) {
            StreamElement streamElement = elementsIterator.next();
            if (streamElement instanceof StreamRecord) {
                output.emitRecord(streamElement.asRecord());
            } else if (streamElement instanceof Watermark) {
                output.emitWatermark(streamElement.asWatermark());
            } else {
                throw new IllegalStateException("Unsupported element type: " + streamElement);
            }
        }
        return elementsIterator.hasNext() ? InputStatus.MORE_AVAILABLE : InputStatus.END_OF_INPUT;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int getInputIndex() {
        return inputIdx;
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
