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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.function.LongFunctionWithException;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A map that keeps track of acquired shared resources and handles their allocation disposal.
 */
final class SharedResources {

	private final ReentrantLock lock = new ReentrantLock();

	@GuardedBy("lock")
	private final HashMap<String, LeasedResource<?>> reservedResources = new HashMap<>();

	/**
	 * Gets the shared memory resource for the given owner and registers a lease. If the resource
	 * does not yet exist, it will be created via the given initializer function.
	 *
	 * <p>The resource must be released when no longer used. That releases the lease. When all leases are
	 * released, the resource is disposed.
	 */
	<T extends AutoCloseable> ResourceAndSize<T> getOrAllocateSharedResource(
			String type,
			Object leaseHolder,
			LongFunctionWithException<T, Exception> initializer,
			long sizeForInitialization) throws Exception {

		// We could be stuck on this lock for a while, in cases where another initialization is currently
		// happening and the initialization is expensive.
		// We lock interruptibly here to allow for faster exit in case of cancellation errors.
		try {
			lock.lockInterruptibly();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new MemoryAllocationException("Interrupted while acquiring memory");
		}

		try {
			// we cannot use "computeIfAbsent()" here because the computing function may throw an exception.
			@SuppressWarnings("unchecked")
			LeasedResource<T> resource = (LeasedResource<T>) reservedResources.get(type);
			if (resource == null) {
				resource = createResource(initializer, sizeForInitialization);
				reservedResources.put(type, resource);
			}

			resource.addLeaseHolder(leaseHolder);
			return resource;
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Releases a lease (identified by the lease holder object) for the given type.
	 * If no further leases exist, the resource is disposed.
	 *
	 * @return True, if this was the last lease holder and the resource was disposed.
	 */
	boolean release(String type, Object leaseHolder) throws Exception {
		lock.lock();
		try {
			final LeasedResource resource = reservedResources.get(type);
			if (resource == null) {
				return false;
			}

			if (resource.removeLeaseHolder(leaseHolder)) {
				reservedResources.remove(type);
				resource.dispose();
				return true;
			}

			return false;
		}
		finally {
			lock.unlock();
		}
	}

	@VisibleForTesting
	int getNumResources() {
		return reservedResources.size();
	}

	private static <T extends AutoCloseable> LeasedResource<T> createResource(
			LongFunctionWithException<T, Exception> initializer,
			long size) throws Exception {

		final T resource = initializer.apply(size);
		return new LeasedResource<>(resource, size);
	}

	// ------------------------------------------------------------------------

	/**
	 * A resource handle with size.
	 */
	interface ResourceAndSize<T extends AutoCloseable> {

		T resourceHandle();

		long size();
	}

	// ------------------------------------------------------------------------

	private static final class LeasedResource<T extends AutoCloseable> implements ResourceAndSize<T> {

		private final HashSet<Object> leaseHolders = new HashSet<>();

		private final T resourceHandle;

		private final long size;

		private boolean disposed;

		private LeasedResource(T resourceHandle, long size) {
			this.resourceHandle = resourceHandle;
			this.size = size;
		}

		public T resourceHandle() {
			return resourceHandle;
		}

		public long size() {
			return size;
		}

		void addLeaseHolder(Object leaseHolder) {
			checkState(!disposed);
			leaseHolders.add(leaseHolder);
		}

		boolean removeLeaseHolder(Object leaseHolder) {
			checkState(!disposed);
			leaseHolders.remove(leaseHolder);
			return leaseHolders.isEmpty();
		}

		void dispose() throws Exception {
			checkState(!disposed);
			disposed = true;
			resourceHandle.close();
		}
	}
}
