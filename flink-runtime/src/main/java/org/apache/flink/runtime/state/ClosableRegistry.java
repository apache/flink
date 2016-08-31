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

public class ClosableRegistry implements Closeable {

	private final Set<Closeable> registeredCloseables;
	private boolean closed;

	public ClosableRegistry() {
		this.registeredCloseables = new HashSet<>();
		this.closed = false;
	}

	public boolean registerClosable(Closeable closeable) {

		if (null == closeable) {
			return false;
		}

		synchronized (getSynchronizationLock()) {
			if (closed) {
				throw new IllegalStateException("Cannot register Closable, registry is already closed.");
			}

			return registeredCloseables.add(closeable);
		}
	}

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

		if (!registeredCloseables.isEmpty()) {

			synchronized (getSynchronizationLock()) {

				for (Closeable closeable : registeredCloseables) {
					IOUtils.closeQuietly(closeable);
				}

				registeredCloseables.clear();
				closed = true;
			}
		}
	}

	private Object getSynchronizationLock() {
		return registeredCloseables;
	}
}