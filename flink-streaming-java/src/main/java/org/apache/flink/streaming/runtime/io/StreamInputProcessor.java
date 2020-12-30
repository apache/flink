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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.streaming.api.operators.InputSelectable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for processing records by {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
 */
@Internal
public interface StreamInputProcessor extends AvailabilityProvider, Closeable {
    /**
     * In case of two and more input processors this method must call {@link
     * InputSelectable#nextSelection()} to choose which input to consume from next.
     *
     * @return input status to estimate whether more records can be processed immediately or not. If
     *     there are no more records available at the moment and the caller should check finished
     *     state and/or {@link #getAvailableFuture()}.
     */
    InputStatus processInput() throws Exception;

    CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws IOException;
}
