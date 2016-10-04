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

package org.apache.flink.runtime.state;

import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * This class allows to register instances of {@link Closeable}, which are all closed if this registry is closed.
 * <p>
 * Registering to an already closed registry will throw an exception and close the provided {@link Closeable}
 * <p>
 * All methods in this class are thread-safe.
 */
public class ClosableRegistry implements Closeable {

	private final Set<Closeable> registeredCloseables;
	private boolean closed;

	public ClosableRegistry() {
		this.registeredCloseables = new HashSet<>();
		this.closed = false;
	}

	/**
	 * Registers a {@link Closeable} with the registry. In case the registry is already closed, this method throws an
	 * {@link IllegalStateException} and closes the passed {@link Closeable}.
	 *
	 * @param closeable Closable tor register
	 * @return true if the the Closable was newly added to the registry
	 * @throws IOException exception when the registry was closed before
	 */
	public boolean registerClosable(Closeable closeable) throws IOException {

		if (null == closeable) {
			return false;
		}

		synchronized (getSynchronizationLock()) {
			if (closed) {
				IOUtils.closeQuietly(closeable);
				throw new IOException("Cannot register Closable, registry is already closed. Closed passed closable.");
			}

			return registeredCloseables.add(closeable);
		}
	}

	/**
	 * Removes a {@link Closeable} from the registry.
	 *
	 * @param closeable instance to remove from the registry.
	 * @return true, if the instance was actually registered and now removed
	 */
	public boolean unregisterClosable(Closeable closeable) {

		if (null == closeable) {
			return false;
		}

		synchronized (getSynchronizationLock()) {
			return registeredCloseables.remove(closeable);
		}
	}

	@Override
	public void close() throws IOException {
		synchronized (getSynchronizationLock()) {

			for (Closeable closeable : registeredCloseables) {
				IOUtils.closeQuietly(closeable);
			}

			registeredCloseables.clear();
			closed = true;
		}
	}

	public boolean isClosed() {
		synchronized (getSynchronizationLock()) {
			return closed;
		}
	}

	private Object getSynchronizationLock() {
		return registeredCloseables;
	}
}
