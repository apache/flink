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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;

import static org.apache.flink.core.memory.MemoryUtils.getByteBufferAddress;

/**
 * This class represents a piece of off-heap memory managed by Flink.
 *
 * <p>The memory can be direct or unsafe, this is transparently handled by this class.
 *
 * <p>Note that memory segments should usually not be allocated manually, but rather through the
 * {@link MemorySegmentFactory}.
 */
@Internal
public abstract class OffHeapMemorySegment extends MemorySegment {
    /**
     * The direct byte buffer that wraps the off-heap memory. This memory segment holds a reference
     * to that buffer, so as long as this memory segment lives, the memory will not be released.
     */
    private ByteBuffer offHeapBuffer;

    /**
     * Creates a new memory segment that represents the memory backing the given direct byte buffer.
     * Note that the given ByteBuffer must be direct {@link
     * java.nio.ByteBuffer#allocateDirect(int)}, otherwise this method with throw an
     * IllegalArgumentException.
     *
     * <p>The memory segment references the given owner.
     *
     * @param buffer The byte buffer whose memory is represented by this memory segment.
     * @param owner The owner references by this memory segment.
     * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
     */
    OffHeapMemorySegment(@Nonnull ByteBuffer buffer, @Nullable Object owner) {
        super(getByteBufferAddress(buffer), buffer.capacity(), owner);
        this.offHeapBuffer = buffer;
    }

    // -------------------------------------------------------------------------
    //  MemorySegment operations
    // -------------------------------------------------------------------------

    @Override
    public void free() {
        super.free();
        offHeapBuffer = null; // to enable GC of unsafe memory
    }

    @Override
    protected ByteBuffer wrapInternal(int offset, int length) {
        if (!isFreed()) {
            try {
                ByteBuffer wrapper = offHeapBuffer.duplicate();
                wrapper.limit(offset + length);
                wrapper.position(offset);
                return wrapper;
            } catch (IllegalArgumentException e) {
                throw new IndexOutOfBoundsException();
            }
        } else {
            throw new IllegalStateException("segment has been freed");
        }
    }
}
