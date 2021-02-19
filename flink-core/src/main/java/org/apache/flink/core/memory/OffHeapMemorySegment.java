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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

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
public final class OffHeapMemorySegment extends MemorySegment {
    /**
     * The direct byte buffer that wraps the off-heap memory. This memory segment holds a reference
     * to that buffer, so as long as this memory segment lives, the memory will not be released.
     */
    @Nullable private ByteBuffer offHeapBuffer;

    @Nullable private final Runnable cleaner;

    /**
     * Wrapping is not allowed when the underlying memory is unsafe. Unsafe memory can be actively
     * released, without reference counting. Therefore, access from wrapped buffers, which may not
     * be aware of the releasing of memory, could be risky.
     */
    private final boolean allowWrap;

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
        this(buffer, owner, true, null);
    }

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
     * @param allowWrap Whether wrapping {@link ByteBuffer}s from the segment is allowed.
     * @param cleaner The cleaner to be called on free segment.
     * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
     */
    OffHeapMemorySegment(
            @Nonnull ByteBuffer buffer,
            @Nullable Object owner,
            boolean allowWrap,
            @Nullable Runnable cleaner) {
        super(getByteBufferAddress(buffer), buffer.capacity(), owner);
        this.offHeapBuffer = buffer;
        this.allowWrap = allowWrap;
        this.cleaner = cleaner;
    }

    // -------------------------------------------------------------------------
    //  MemorySegment operations
    // -------------------------------------------------------------------------

    @Override
    public void free() {
        super.free();
        if (cleaner != null) {
            cleaner.run();
        }
        offHeapBuffer = null; // to enable GC of unsafe memory
    }

    @Override
    public ByteBuffer wrap(int offset, int length) {
        if (!allowWrap) {
            throw new UnsupportedOperationException(
                    "Wrap is not supported by this segment. This usually indicates that the underlying memory is unsafe, thus transferring of ownership is not allowed.");
        }
        return wrapInternal(offset, length);
    }

    private ByteBuffer wrapInternal(int offset, int length) {
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

    @Override
    public <T> T processAsByteBuffer(Function<ByteBuffer, T> processFunction) {
        return Preconditions.checkNotNull(processFunction).apply(wrapInternal(0, size()));
    }

    @Override
    public void processAsByteBuffer(Consumer<ByteBuffer> processConsumer) {
        Preconditions.checkNotNull(processConsumer).accept(wrapInternal(0, size()));
    }
}
