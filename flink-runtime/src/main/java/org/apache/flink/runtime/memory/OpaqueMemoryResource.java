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

package org.apache.flink.runtime.memory;

import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An opaque memory resource, meaning a memory resource not understood by Flink or the JVM.
 * An example for this is a native resource, like RocksDB's block cache memory pool.
 *
 * <p>The resource must be closed after it is not used any more.
 */
public final class OpaqueMemoryResource<T> implements AutoCloseable {

	private final T resourceHandle;

	private final long size;

	private final ThrowingRunnable<Exception> disposer;

	private final AtomicBoolean closed = new AtomicBoolean();

	public OpaqueMemoryResource(T resourceHandle, long size, ThrowingRunnable<Exception> disposer) {
		checkArgument(size >= 0, "size must be >= 0");
		this.resourceHandle = checkNotNull(resourceHandle, "resourceHandle");
		this.disposer = checkNotNull(disposer, "disposer");
		this.size = size;
	}

	/**
	 * Gets the handle to the resource.
	 */
	public T getResourceHandle() {
		return resourceHandle;
	}

	/**
	 * Gets the size, in bytes.
	 */
	public long getSize() {
		return size;
	}

	/**
	 * Releases this resource.
	 * This method is idempotent.
	 */
	@Override
	public void close() throws Exception {
		if (closed.compareAndSet(false, true)) {
			disposer.run();
		}
	}

	@Override
	public String toString() {
		return "OpaqueMemoryResource (" + size + " bytes) @ " + resourceHandle + (closed.get() ? " (disposed)" : "");
	}
}
