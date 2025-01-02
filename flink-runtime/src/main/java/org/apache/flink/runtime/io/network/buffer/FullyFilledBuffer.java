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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link Buffer} represents a fully filled buffer which contains multiple
 * partial buffers for network data communication.
 *
 * <p>All partial must have the same data type and compression status. And they are originate from
 * different write buffers but are combined to minimize network overhead.
 */
public class FullyFilledBuffer extends AbstractCompositeBuffer {

    public FullyFilledBuffer(DataType dataType, int length, boolean isCompressed) {
        super(dataType, length, isCompressed);
    }

    @Override
    public ByteBuf asByteBuf() {
        CompositeByteBuf compositeByteBuf = checkNotNull(allocator).compositeDirectBuffer();
        for (Buffer buffer : partialBuffers) {
            if (buffer instanceof CompositeBuffer) {
                buffer.setAllocator(allocator);
            }
            compositeByteBuf.addComponent(buffer.asByteBuf());
        }
        compositeByteBuf.writerIndex(currentLength);
        return compositeByteBuf;
    }

    @Override
    public void addPartialBuffer(Buffer buffer) {
        checkState(
                buffer.getDataType() == dataType,
                "Partial buffer data type must be the same as the fully filled buffer.");
        checkState(
                buffer.isCompressed() == isCompressed,
                "Partial buffer compression status must be the same as the fully filled buffer.");

        partialBuffers.add(buffer);
        currentLength += buffer.readableBytes();
        checkState(currentLength <= length);
    }
}
