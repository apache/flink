/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is a guard for shared resources with the following invariants. The resource can be acquired by multiple
 * clients in parallel through the {@link #acquireResource()} call. As a result of the call, each client gets a
 * {@link Lease}. The {@link #close()} method of the lease releases the resources and reduces the client count in
 * the {@link ResourceGuard} object.
 * The protected resource should only be disposed once the corresponding resource guard is successfully closed, but
 * the guard can only be closed once all clients that acquired a lease for the guarded resource have released it.
 * Before this is happened, the call to {@link #close()} will block until the zero-open-leases condition is triggered.
 * After the resource guard is closed, calls to {@link #acquireResource()} will fail with exception. Notice that,
 * obviously clients are responsible to release the resource after usage. All clients are considered equal, i.e. there
 * is only a global count maintained how many times the resource was acquired but not by whom.
 */
public class ResourceGuard implements AutoCloseable, Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The object that serves as lock for count and the closed-flag.
	 */
	private final SerializableObject lock;

	/**
	 * Number of clients that have ongoing access to the resource.
	 */
	private volatile int leaseCount;

	/**
	 * This flag indicated if it is still possible to acquire access to the resource.
	 */
	private volatile boolean closed;

	public ResourceGuard() {
		this.lock = new SerializableObject();
		this.leaseCount = 0;
		this.closed = false;
	}

	/**
	 * Acquired access from one new client for the guarded resource.
	 *
	 * @throws IOException when the resource guard is already closed.
	 */
	public Lease acquireResource() throws IOException {

		synchronized (lock) {

			if (closed) {
				throw new IOException("Resource guard was already closed.");
			}

			++leaseCount;
		}

		return new Lease();
	}

	/**
	 * Releases access for one client of the guarded resource. This method must only be called after a matching call to
	 * {@link #acquireResource()}.
	 */
	private void releaseResource() {

		synchronized (lock) {

			--leaseCount;

			if (closed && leaseCount == 0) {
				lock.notifyAll();
			}
		}
	}

	/**
	 * Closed the resource guard. This method will block until all calls to {@link #acquireResource()} have seen their
	 * matching call to {@link #releaseResource()}.
	 */
	@Override
	public void close() {

		synchronized (lock) {

			closed = true;

			while (leaseCount > 0) {

				try {
					lock.wait();
				} catch (InterruptedException ignore) {
					// Even on interruption, we cannot terminate the loop until all open leases are closed.
				}
			}
		}
	}

	/**
	 * Returns true if the resource guard is closed, i.e. after {@link #close()} was called.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Returns the current count of open leases.
	 */
	public int getLeaseCount() {
		return leaseCount;
	}

	/**
	 * A lease is issued by the {@link ResourceGuard} as result of calls to {@link #acquireResource()}. The owner of the
	 * lease can release it via the {@link #close()} call.
	 */
	public class Lease implements AutoCloseable {

		private final AtomicBoolean closed;

		private Lease() {
			this.closed = new AtomicBoolean(false);
		}

		@Override
		public void close() {
			if (closed.compareAndSet(false, true)) {
				releaseResource();
			}
		}
	}
}
