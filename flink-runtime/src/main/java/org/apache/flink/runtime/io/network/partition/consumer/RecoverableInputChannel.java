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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/** Physical input channel that can receive recovered buffers pushed by the spill drain. */
@Internal
public interface RecoverableInputChannel {

    InputChannelInfo getChannelInfo();

    /**
     * Appends a recovered buffer or recovery-checkpoint sentinel. Released channels recycle the
     * buffer silently.
     */
    void onRecoveredStateBuffer(Buffer buffer);

    /**
     * Marks producer-side recovery delivery complete. Implementations wait for upstream readiness
     * before flipping this state so channels without spill entries still observe the same handoff.
     */
    void finishRecoveredBufferDelivery() throws IOException, InterruptedException;

    /**
     * Inserts a {@code RecoveryCheckpointBarrier} for {@code checkpointId} into this channel's
     * recovery queue if the channel is still in recovery.
     */
    void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId) throws IOException;

    /**
     * Blocks until a buffer is available from this channel's own buffer pool. Implementations must
     * first await upstream readiness and must be invoked outside the drainer lock.
     */
    Buffer requestRecoveryBufferBlocking() throws InterruptedException, IOException;

    /**
     * Invoked by the consume path the moment it polls the {@code EndOfFetchedChannelStateEvent}
     * sentinel, i.e. once all recovered buffers have been consumed. Implementations flip out of
     * recovery, release any upstream events held back during recovery, and reopen the upstream so
     * live data may flow again.
     */
    void onRecoveredStateConsumed() throws IOException;
}
