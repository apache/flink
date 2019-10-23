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

import java.nio.ByteBuffer;

/**
 * A factory for (hybrid) memory segments ({@link HybridMemorySegment}).
 *
 * <p>The purpose of this factory is to make sure that all memory segments for heap data are of the
 * same type. That way, the runtime does not mix the various specializations of the {@link
 * MemorySegment}. Not mixing them has shown to be beneficial to method specialization by the JIT
 * and to overall performance.
 */
@Internal
public final class MemorySegmentFactory {

	/**
	 * Creates a new memory segment that targets the given heap memory region.
	 *
	 * <p>This method should be used to turn short lived byte arrays into memory segments.
	 *
	 * @param buffer The heap memory region.
	 * @return A new memory segment that targets the given heap memory region.
	 */
	public static MemorySegment wrap(byte[] buffer) {
		return new HybridMemorySegment(buffer, null);
	}

	/**
	 * Allocates some unpooled memory and creates a new memory segment that represents
	 * that memory.
	 *
	 * <p>This method is similar to {@link #allocateUnpooledSegment(int, Object)}, but the
	 * memory segment will have null as the owner.
	 *
	 * @param size The size of the memory segment to allocate.
	 * @return A new memory segment, backed by unpooled heap memory.
	 */
	public static MemorySegment allocateUnpooledSegment(int size) {
		return allocateUnpooledSegment(size, null);
	}

	/**
	 * Allocates some unpooled memory and creates a new memory segment that represents
	 * that memory.
	 *
	 * <p>This method is similar to {@link #allocateUnpooledSegment(int)}, but additionally sets
	 * the owner of the memory segment.
	 *
	 * @param size The size of the memory segment to allocate.
	 * @param owner The owner to associate with the memory segment.
	 * @return A new memory segment, backed by unpooled heap memory.
	 */
	public static MemorySegment allocateUnpooledSegment(int size, Object owner) {
		return new HybridMemorySegment(new byte[size], owner);
	}

	/**
	 * Allocates some unpooled off-heap memory and creates a new memory segment that
	 * represents that memory.
	 *
	 * @param size The size of the off-heap memory segment to allocate.
	 * @return A new memory segment, backed by unpooled off-heap memory.
	 */
	public static MemorySegment allocateUnpooledOffHeapMemory(int size) {
		return allocateUnpooledOffHeapMemory(size, null);
	}

	/**
	 * Allocates some unpooled off-heap memory and creates a new memory segment that
	 * represents that memory.
	 *
	 * @param size The size of the off-heap memory segment to allocate.
	 * @param owner The owner to associate with the off-heap memory segment.
	 * @return A new memory segment, backed by unpooled off-heap memory.
	 */
	public static MemorySegment allocateUnpooledOffHeapMemory(int size, Object owner) {
		ByteBuffer memory = ByteBuffer.allocateDirect(size);
		return new HybridMemorySegment(memory, owner, null);
	}

	/**
	 * Allocates an off-heap unsafe memory and creates a new memory segment to represent that memory.
	 *
	 * <p>Creation of this segment schedules its memory freeing operation when its java wrapping object is about
	 * to be garbage collected, similar to {@link java.nio.DirectByteBuffer#DirectByteBuffer(int)}.
	 * The difference is that this memory allocation is out of option -XX:MaxDirectMemorySize limitation.
	 *
	 * @param size The size of the off-heap unsafe memory segment to allocate.
	 * @param owner The owner to associate with the off-heap unsafe memory segment.
	 * @return A new memory segment, backed by off-heap unsafe memory.
	 */
	public static MemorySegment allocateOffHeapUnsafeMemory(int size, Object owner) {
		long address = MemoryUtils.allocateUnsafe(size);
		ByteBuffer offHeapBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, size);
		return new HybridMemorySegment(offHeapBuffer, owner, MemoryUtils.createMemoryGcCleaner(offHeapBuffer, address));
	}

	/**
	 * Creates a memory segment that wraps the off-heap memory backing the given ByteBuffer.
	 * Note that the ByteBuffer needs to be a <i>direct ByteBuffer</i>.
	 *
	 * <p>This method is intended to be used for components which pool memory and create
	 * memory segments around long-lived memory regions.
	 *
	 * @param memory The byte buffer with the off-heap memory to be represented by the memory segment.
	 * @return A new memory segment representing the given off-heap memory.
	 */
	public static MemorySegment wrapOffHeapMemory(ByteBuffer memory) {
		return new HybridMemorySegment(memory, null, null);
	}

}
