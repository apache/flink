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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecoveredBufferStoreCoordinator;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

/** No-op {@link RecoveredBufferStore} sentinel for channels without recovered data. */
class EmptyRecoveredBufferStore implements RecoveredBufferStore {

    @Nullable
    @Override
    public Buffer tryTake() {
        return null;
    }

    @Override
    public Buffer.DataType peekNextDataType() {
        return Buffer.DataType.NONE;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void checkpoint(ChannelStateWriter writer, long checkpointId) {}

    @Override
    public void releaseAll() {}

    @Override
    public void notifyCheckpointStopped(long checkpointId) {}

    @Override
    public void setCoordinator(RecoveredBufferStoreCoordinator coordinator) {}

    @Override
    public void setDataAvailableListener(DataAvailableListener listener) {}
}
