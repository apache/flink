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

import java.nio.ByteBuffer;

/**
 * A factory for memory segments. The purpose of this factory is to make sure that all memory segments
 * for heap data are of the same type. That way, the runtime does not mix the various specializations
 * of the {@link org.apache.flink.core.memory.MemorySegment}. Not mixing them has shown to be beneficial
 * to method specialization by the JIT and to overall performance.
 * <p>
 * Note that this factory auto-initialized to use {@link org.apache.flink.core.memory.HeapMemorySegment},
 * if a request to create a segment comes before the initialization.
 */
public class MemorySegmentFactory {

	/** The factory to use */
	private static Factory factory;
	
	/**
	 * Creates a new memory segment that targets the given heap memory region.
	 *
	 * @param buffer The heap memory region.
	 * @return A new memory segment that targets the given heap memory region.
	 * @throws java.lang.IllegalStateException Thrown if the factory has not been initialized.
	 */
	public static MemorySegment wrap(byte[] buffer) {
		ensureInitialized();
		return factory.wrap(buffer);
	}

	public static MemorySegment allocateUnpooledSegment(int size) {
		return allocateUnpooledSegment(size, null);
	}
	
	public static MemorySegment allocateUnpooledSegment(int size, Object owner) {
		ensureInitialized();
		return factory.allocateUnpooledSegment(size, owner);
	}
	
	public static MemorySegment wrapPooledHeapMemory(byte[] memory, Object owner) {
		ensureInitialized();
		return factory.wrapPooledHeapMemory(memory, owner);
	}

	public static MemorySegment wrapPooledOffHeapMemory(ByteBuffer memory, Object owner) {
		ensureInitialized();
		return factory.wrapPooledOffHeapMemory(memory, owner);
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Initializes this factory with the given concrete factory.
	 * 
	 * @param f The concrete factory to use.
	 * @throws java.lang.IllegalStateException Thrown, if this factory has been initialized before.
	 */
	public static void initializeFactory(Factory f) {
		if (f == null) {
			throw new NullPointerException();
		}
	
		synchronized (MemorySegmentFactory.class) {
			if (factory == null) {
				factory = f;
			}
			else {
				throw new IllegalStateException("Factory has already been initialized");
			}
		}
	}

	/**
	 * Checks whether this memory segment factory has been initialized (with a type to produce).
	 * @return True, if the factory has been initialized, false otherwise.
	 */
	public static boolean isInitialized() {
		return factory != null;
	}

	public static Factory getFactory() {
		return factory;
	}
	
	private static void ensureInitialized() {
		if (factory == null) {
			factory = HeapMemorySegment.FACTORY;
		}
	}

	// ------------------------------------------------------------------------
	//  Internal factory
	// ------------------------------------------------------------------------
	
	/**
	 * A concrete factory for memory segments.
	 */
	public static interface Factory {

		/**
		 * Creates a new memory segment that targets the given heap memory region.
		 *
		 * @param memory The heap memory region.
		 * @return A new memory segment that targets the given heap memory region.
		 */
		MemorySegment wrap(byte[] memory);


		MemorySegment allocateUnpooledSegment(int size, Object owner);


		MemorySegment wrapPooledHeapMemory(byte[] memory, Object owner);


		MemorySegment wrapPooledOffHeapMemory(ByteBuffer memory, Object owner);
	}
}
