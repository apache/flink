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

/**
 * This class represents a piece of direct off-heap memory managed by Flink.
 *
 * <p>Note that memory segments should usually not be allocated manually, but rather through the
 * {@link MemorySegmentFactory}.
 */
@Internal
public final class DirectMemorySegment extends OffHeapMemorySegment {

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
    DirectMemorySegment(@Nonnull ByteBuffer buffer, @Nullable Object owner) {
        super(buffer, owner);
    }

    @Override
    public ByteBuffer wrap(int offset, int length) {
        return wrapInternal(offset, length);
    }
}
