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

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the abstract base class for registries that allow to register instances of {@link Closeable}, which are
 * all closed if this registry is closed.
 * <p>
 * Registering to an already closed registry will throw an exception and close the provided {@link Closeable}
 * <p>
 * All methods in this class are thread-safe.
 *
 * @param <C> Type of the {@link Closeable} this registers
 * @param <T> Type for potential meta data associated with the registering closeables
 */
public abstract class AbstractConcurrentClosingRegistry<C extends Closeable, T> implements Closeable {

	protected final Map<Closeable, T> closeableToRef;
	protected boolean closed;

	public AbstractConcurrentClosingRegistry() {
		this(new HashMap<Closeable, T>());
	}

	public AbstractConcurrentClosingRegistry(Map<Closeable, T> closeableToRef) {
		this.closeableToRef = Preconditions.checkNotNull(closeableToRef);
		this.closed = false;
	}

	/**
	 * Registers an {@link Closeable} with the registry. In case the registry is already closed, this method throws an
	 * {@link IllegalStateException} and closes the passed {@link Closeable}.
	 *
	 * @param closeable {@link Closeable} to register
	 * @throws IOException exception when the registry was closed before
	 */
	public final void registerClosable(C closeable) throws IOException {

		if (null == closeable) {
			return;
		}

		synchronized (getSynchronizationLock()) {
			doRegistering(closeable, closeableToRef);
		}
	}

	/**
	 * Removes a {@link Closeable} from the registry.
	 *
	 * @param closeable instance to remove from the registry.
	 */
	public final void unregisterClosable(C closeable) {

		if (null == closeable) {
			return;
		}

		synchronized (getSynchronizationLock()) {
			doUnRegistering(closeable, closeableToRef);
		}
	}

	@Override
	public final void close() throws IOException {
		synchronized (getSynchronizationLock()) {
			if (!closed) {
				closed = true;
				doClosing(closeableToRef);
			}
		}
	}

	public boolean isClosed() {
		synchronized (getSynchronizationLock()) {
			return closed;
		}
	}

	protected final Object getSynchronizationLock() {
		return closeableToRef;
	}

	@GuardedBy("closeableToRef")
	protected abstract void doUnRegistering(C closeable, Map<Closeable, T> closeableMap);

	@GuardedBy("closeableToRef")
	protected abstract void doRegistering(C closeable, Map<Closeable, T> closeableMap) throws IOException;

	@GuardedBy("closeableToRef")
	protected abstract void doClosing(Map<Closeable, T> closeableMap) throws IOException;
}
