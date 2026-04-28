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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.io.IOException;

/** Supplies per-channel network buffers to the recovery pipeline. */
@Internal
interface BufferRequester {

    /** Non-blocking; returns {@code null} when no buffer is currently available. */
    @Nullable
    Buffer requestBuffer(InputChannelInfo channelInfo) throws IOException;

    Buffer requestBufferBlocking(InputChannelInfo channelInfo)
            throws InterruptedException, IOException;

    /**
     * Releases exclusive buffers for every channel served by this requester. Idempotent. Must run
     * after the dispatcher's drain has finished so the underlying pools are no longer being read
     * from.
     */
    void releaseExclusiveBuffers() throws IOException;
}
