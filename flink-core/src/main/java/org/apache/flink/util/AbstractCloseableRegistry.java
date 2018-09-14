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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * This is the abstract base class for registries that allow to register instances of {@link Closeable}, which are all
 * closed if this registry is closed.
 *
 * <p>Registering to an already closed registry will throw an exception and close the provided {@link Closeable}
 *
 * <p>All methods in this class are thread-safe.
 *
 * @param <C> Type of the closeable this registers
 * @param <T> Type for potential meta data associated with the registering closeables
 */
@Internal
public abstract class AbstractCloseableRegistry<C extends Closeable, T> implements Closeable {

	/** Lock that guards state of this registry. **/
	private final Object lock;

	/** Map from tracked Closeables to some associated meta data. */
	@GuardedBy("lock")
	private final Map<Closeable, T> closeableToRef;

	/** Indicates if this registry is closed. */
	@GuardedBy("lock")
	private boolean closed;

	public AbstractCloseableRegistry(@Nonnull Map<Closeable, T> closeableToRef) {
		this.lock = new Object();
		this.closeableToRef = Preconditions.checkNotNull(closeableToRef);
		this.closed = false;
	}

	/**
	 * Registers a {@link Closeable} with the registry. In case the registry is already closed, this method throws an
	 * {@link IllegalStateException} and closes the passed {@link Closeable}.
	 *
	 * @param closeable Closeable tor register
	 * @throws IOException exception when the registry was closed before
	 */
	public final void registerCloseable(C closeable) throws IOException {

		if (null == closeable) {
			return;
		}

		synchronized (getSynchronizationLock()) {
			if (!closed) {
				doRegister(closeable, closeableToRef);
				return;
			}
		}

		IOUtils.closeQuietly(closeable);
		throw new IOException("Cannot register Closeable, registry is already closed. Closing argument.");
	}

	/**
	 * Removes a {@link Closeable} from the registry.
	 *
	 * @param closeable instance to remove from the registry.
	 * @return true if the closeable was previously registered and became unregistered through this call.
	 */
	public final boolean unregisterCloseable(C closeable) {

		if (null == closeable) {
			return false;
		}

		synchronized (getSynchronizationLock()) {
			return doUnRegister(closeable, closeableToRef);
		}
	}

	@Override
	public void close() throws IOException {
		Collection<Closeable> toCloseCopy;

		synchronized (getSynchronizationLock()) {

			if (closed) {
				return;
			}

			closed = true;

			toCloseCopy = new ArrayList<>(closeableToRef.keySet());

			closeableToRef.clear();
		}

		IOUtils.closeAllQuietly(toCloseCopy);
	}

	public boolean isClosed() {
		synchronized (getSynchronizationLock()) {
			return closed;
		}
	}

	/**
	 * Does the actual registration of the closeable with the registry map. This should not do any long running or
	 * potentially blocking operations as is is executed under the registry's lock.
	 */
	protected abstract void doRegister(@Nonnull C closeable, @Nonnull Map<Closeable, T> closeableMap);

	/**
	 * Does the actual un-registration of the closeable from the registry map. This should not do any long running or
	 * potentially blocking operations as is is executed under the registry's lock.
	 */
	protected abstract boolean doUnRegister(@Nonnull C closeable, @Nonnull Map<Closeable, T> closeableMap);

	/**
	 * Returns the lock on which manipulations to members closeableToRef and closeable must be synchronized.
	 */
	protected final Object getSynchronizationLock() {
		return lock;
	}

	/**
	 * Adds a mapping to the registry map, respecting locking.
	 */
	protected final void addCloseableInternal(Closeable closeable, T metaData) {
		synchronized (getSynchronizationLock()) {
			closeableToRef.put(closeable, metaData);
		}
	}

	/**
	 * Removes a mapping from the registry map, respecting locking.
	 */
	protected final void removeCloseableInternal(Closeable closeable) {
		synchronized (getSynchronizationLock()) {
			closeableToRef.remove(closeable);
		}
	}

	@VisibleForTesting
	public final int getNumberOfRegisteredCloseables() {
		synchronized (getSynchronizationLock()) {
			return closeableToRef.size();
		}
	}

	@VisibleForTesting
	public final boolean isCloseableRegistered(Closeable c) {
		synchronized (getSynchronizationLock()) {
			return closeableToRef.containsKey(c);
		}
	}
}
