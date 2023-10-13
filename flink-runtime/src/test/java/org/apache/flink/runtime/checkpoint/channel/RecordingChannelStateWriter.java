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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava31.com.google.common.collect.LinkedListMultimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ListMultimap;

import java.util.Arrays;

import static org.apache.flink.util.ExceptionUtils.rethrow;

/** A simple {@link ChannelStateWriter} used to write unit tests. */
public class RecordingChannelStateWriter extends MockChannelStateWriter {
    private long lastStartedCheckpointId = -1;
    private final ListMultimap<InputChannelInfo, Buffer> addedInput = LinkedListMultimap.create();
    private final ListMultimap<ResultSubpartitionInfo, Buffer> addedOutput =
            LinkedListMultimap.create();

    public RecordingChannelStateWriter() {
        super(false);
    }

    public void reset() {
        lastStartedCheckpointId = -1;
        addedInput.values().forEach(Buffer::recycleBuffer);
        addedInput.clear();
        addedOutput.values().forEach(Buffer::recycleBuffer);
        addedOutput.clear();
    }

    @Override
    public void start(long checkpointId, CheckpointOptions checkpointOptions) {
        super.start(checkpointId, checkpointOptions);
        lastStartedCheckpointId = checkpointId;
    }

    @Override
    public void addInputData(
            long checkpointId,
            InputChannelInfo info,
            int startSeqNum,
            CloseableIterator<Buffer> iterator) {
        checkCheckpointId(checkpointId);
        iterator.forEachRemaining(b -> addedInput.put(info, b));
        try {
            iterator.close();
        } catch (Exception e) {
            rethrow(e);
        }
    }

    @Override
    public void addOutputData(
            long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
        checkCheckpointId(checkpointId);
        addedOutput.putAll(info, Arrays.asList(data));
    }

    public long getLastStartedCheckpointId() {
        return lastStartedCheckpointId;
    }

    public ListMultimap<InputChannelInfo, Buffer> getAddedInput() {
        return addedInput;
    }

    public ListMultimap<ResultSubpartitionInfo, Buffer> getAddedOutput() {
        return addedOutput;
    }
}
