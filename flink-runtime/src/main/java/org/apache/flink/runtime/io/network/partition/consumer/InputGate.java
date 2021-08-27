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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 */
public abstract class InputGate
        implements PullingAsyncDataInput<BufferOrEvent>, AutoCloseable, ChannelStateHolder {

    protected final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    protected final AvailabilityHelper priorityAvailabilityHelper = new AvailabilityHelper();

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (int index = 0, numChannels = getNumberOfInputChannels();
                index < numChannels;
                index++) {
            final InputChannel channel = getChannel(index);
            if (channel instanceof ChannelStateHolder) {
                ((ChannelStateHolder) channel).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    public abstract int getNumberOfInputChannels();

    public abstract boolean isFinished();

    public abstract boolean hasReceivedEndOfData();

    /**
     * Blocking call waiting for next {@link BufferOrEvent}.
     *
     * <p>Note: It should be guaranteed that the previous returned buffer has been recycled before
     * getting next one.
     *
     * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
     */
    public abstract Optional<BufferOrEvent> getNext() throws IOException, InterruptedException;

    /**
     * Poll the {@link BufferOrEvent}.
     *
     * <p>Note: It should be guaranteed that the previous returned buffer has been recycled before
     * polling next one.
     *
     * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()}
     *     returns true.
     */
    public abstract Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException;

    public abstract void sendTaskEvent(TaskEvent event) throws IOException;

    /**
     * @return a future that is completed if there are more records available. If there are more
     *     records available immediately, {@link #AVAILABLE} should be returned. Previously returned
     *     not completed futures should become completed once there are more records available.
     */
    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    public abstract void resumeConsumption(InputChannelInfo channelInfo) throws IOException;

    public abstract void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo)
            throws IOException;

    /** Returns the channel of this gate. */
    public abstract InputChannel getChannel(int channelIndex);

    /** Returns the channel infos of this gate. */
    public List<InputChannelInfo> getChannelInfos() {
        return IntStream.range(0, getNumberOfInputChannels())
                .mapToObj(index -> getChannel(index).getChannelInfo())
                .collect(Collectors.toList());
    }

    /**
     * Notifies when a priority event has been enqueued. If this future is queried from task thread,
     * it is guaranteed that a priority event is available and retrieved through {@link #getNext()}.
     */
    public CompletableFuture<?> getPriorityEventAvailableFuture() {
        return priorityAvailabilityHelper.getAvailableFuture();
    }

    /** Simple pojo for INPUT, DATA and moreAvailable. */
    protected static class InputWithData<INPUT, DATA> {
        protected final INPUT input;
        protected final DATA data;
        protected final boolean moreAvailable;
        protected final boolean morePriorityEvents;

        InputWithData(INPUT input, DATA data, boolean moreAvailable, boolean morePriorityEvents) {
            this.input = checkNotNull(input);
            this.data = checkNotNull(data);
            this.moreAvailable = moreAvailable;
            this.morePriorityEvents = morePriorityEvents;
        }

        @Override
        public String toString() {
            return "InputWithData{"
                    + "input="
                    + input
                    + ", data="
                    + data
                    + ", moreAvailable="
                    + moreAvailable
                    + ", morePriorityEvents="
                    + morePriorityEvents
                    + '}';
        }
    }

    /** Setup gate, potentially heavy-weight, blocking operation comparing to just creation. */
    public abstract void setup() throws IOException;

    public abstract void requestPartitions() throws IOException;

    public abstract CompletableFuture<Void> getStateConsumedFuture();

    public abstract void finishReadRecoveredState() throws IOException;
}
