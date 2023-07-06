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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

/** The default implementation of {@link NettyConnectionReader}. */
public class NettyConnectionReaderImpl implements NettyConnectionReader {

    /** The index of input channel related to the reader. */
    private final int inputChannelIndex;

    /** The provider to provide the input channel. */
    private final Supplier<InputChannel> inputChannelProvider;

    /** The helper is used to notify the available and priority status of reader. */
    private final NettyConnectionReaderAvailabilityAndPriorityHelper helper;

    /** The last required segment id. */
    private int lastRequiredSegmentId = 0;

    public NettyConnectionReaderImpl(
            int inputChannelIndex,
            Supplier<InputChannel> inputChannelProvider,
            NettyConnectionReaderAvailabilityAndPriorityHelper helper) {
        this.inputChannelIndex = inputChannelIndex;
        this.inputChannelProvider = inputChannelProvider;
        this.helper = helper;
    }

    @Override
    public Optional<Buffer> readBuffer(int segmentId) {
        if (segmentId > 0L && (segmentId != lastRequiredSegmentId)) {
            lastRequiredSegmentId = segmentId;
            try {
                inputChannelProvider.get().notifyRequiredSegmentId(segmentId);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to notify required segment id");
            }
        }
        Optional<InputChannel.BufferAndAvailability> bufferAndAvailability = Optional.empty();
        try {
            bufferAndAvailability = inputChannelProvider.get().getNextBuffer();
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to read buffer.");
        }
        if (bufferAndAvailability.isPresent()) {
            if (bufferAndAvailability.get().moreAvailable()) {
                helper.notifyReaderAvailableAndPriority(
                        inputChannelIndex, bufferAndAvailability.get().hasPriority());
            }
            if (bufferAndAvailability.get().hasPriority()) {
                helper.updatePrioritySequenceNumber(
                        inputChannelIndex, bufferAndAvailability.get().getSequenceNumber());
            }
        }
        return bufferAndAvailability.map(InputChannel.BufferAndAvailability::buffer);
    }
}
