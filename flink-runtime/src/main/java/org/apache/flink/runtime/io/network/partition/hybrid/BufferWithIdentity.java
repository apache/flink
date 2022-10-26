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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.Buffer;

/** Integrate the buffer with index and the channel information to which it belongs. */
public class BufferWithIdentity {
    private final Buffer buffer;

    private final int bufferIndex;

    private final int channelIndex;

    public BufferWithIdentity(Buffer buffer, int bufferIndex, int channelIndex) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.channelIndex = channelIndex;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getChannelIndex() {
        return channelIndex;
    }
}
