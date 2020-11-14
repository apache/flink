/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.ServiceConnectionManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Base class for service connection managers, taking care of the connection handling.
 */
public class AbstractServiceConnectionManager<S> implements ServiceConnectionManager<S> {

	protected final Object lock = new Object();

	@Nullable
	@GuardedBy("lock")
	protected S service;

	@GuardedBy("lock")
	private boolean closed = false;

	@Override
	public final void connect(S service) {
		synchronized (lock) {
			checkNotClosed();
			this.service = service;
		}
	}

	@Override
	public final void disconnect() {
		synchronized (lock) {
			checkNotClosed();
			this.service = null;
		}
	}

	@Override
	public final void close() {
		synchronized (lock) {
			closed = true;
			service = null;
		}
	}

	protected final void checkNotClosed() {
		if (closed) {
			throw new IllegalStateException("This connection manager has already been closed.");
		}
	}

	protected final boolean isConnected() {
		return service != null;
	}
}
