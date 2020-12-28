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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@ThreadSafe
public class TestBufferFactory {

    public static final int BUFFER_SIZE = 32 * 1024;

    private static final BufferRecycler RECYCLER = FreeingBufferRecycler.INSTANCE;

    private final int bufferSize;

    private final BufferRecycler bufferRecycler;

    private final int poolSize;

    private int numberOfCreatedBuffers = 0;

    public TestBufferFactory(int poolSize, int bufferSize, BufferRecycler bufferRecycler) {
        checkArgument(bufferSize > 0);
        this.poolSize = poolSize;
        this.bufferSize = bufferSize;
        this.bufferRecycler = checkNotNull(bufferRecycler);
    }

    public synchronized Buffer create() {
        if (numberOfCreatedBuffers >= poolSize) {
            return null;
        }

        numberOfCreatedBuffers++;
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(bufferSize), bufferRecycler);
    }

    public synchronized int getNumberOfCreatedBuffers() {
        return numberOfCreatedBuffers;
    }

    // ------------------------------------------------------------------------
    // Static test helpers
    // ------------------------------------------------------------------------

    /**
     * Creates a (network) buffer with default size, i.e. {@link #BUFFER_SIZE}, and unspecified data
     * of the given size.
     *
     * @param dataSize size of the data in the buffer, i.e. the new writer index
     * @return a new buffer instance
     */
    public static Buffer createBuffer(int dataSize) {
        return createBuffer(BUFFER_SIZE, dataSize);
    }

    /**
     * Creates a (network) buffer with unspecified data of the given size.
     *
     * @param bufferSize size of the buffer
     * @param dataSize size of the data in the buffer, i.e. the new writer index
     * @return a new buffer instance
     */
    public static Buffer createBuffer(int bufferSize, int dataSize) {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(bufferSize),
                RECYCLER,
                Buffer.DataType.DATA_BUFFER,
                dataSize);
    }
}
