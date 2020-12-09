/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapping {@link StreamTaskInput} that invokes a given {@link BoundedMultiInput} when reaching
 * {@link InputStatus#END_OF_INPUT}.
 */
class ObservableStreamTaskInput<T> implements StreamTaskInput<T> {

    private final StreamTaskInput<T> wrappedInput;
    private final BoundedMultiInput endOfInputObserver;

    public ObservableStreamTaskInput(
            StreamTaskInput<T> wrappedInput, BoundedMultiInput endOfInputObserver) {
        this.wrappedInput = wrappedInput;
        this.endOfInputObserver = endOfInputObserver;
    }

    @Override
    public InputStatus emitNext(DataOutput<T> output) throws Exception {
        InputStatus result = wrappedInput.emitNext(output);
        if (result == InputStatus.END_OF_INPUT) {
            endOfInputObserver.endInput(wrappedInput.getInputIndex());
        }
        return result;
    }

    @Override
    public int getInputIndex() {
        return wrappedInput.getInputIndex();
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws IOException {
        return wrappedInput.prepareSnapshot(channelStateWriter, checkpointId);
    }

    @Override
    public void close() throws IOException {
        wrappedInput.close();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return wrappedInput.getAvailableFuture();
    }

    @Override
    public boolean isAvailable() {
        return wrappedInput.isAvailable();
    }
}
