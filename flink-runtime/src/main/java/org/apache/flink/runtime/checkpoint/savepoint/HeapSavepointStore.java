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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Heap-backed savepoint store.
 *
 * <p>The life-cycle of savepoints is bound to the life-cycle of the cluster.
 */
public class HeapSavepointStore implements SavepointStore {

	private static final Logger LOG = LoggerFactory.getLogger(HeapSavepointStore.class);

	private final Object shutDownLock = new Object();

	/** Stored savepoints. */
	private final Map<String, Savepoint> savepoints = new HashMap<>(1);

	/** ID counter to identify savepoints. */
	private final AtomicInteger currentId = new AtomicInteger();

	/** Flag indicating whether state store has been shut down. */
	private boolean shutDown;

	/** Shut down hook. */
	private final Thread shutdownHook;

	/**
	 * Creates a heap-backed savepoint store.
	 *
	 * <p>Savepoint are discarded on {@link #shutdown()}.
	 */
	public HeapSavepointStore() {
		this.shutdownHook = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					shutdown();
				} catch (Throwable t) {
					LOG.warn("Failure during shut down hook.", t);
				}
			}
		});

		try {
			Runtime.getRuntime().addShutdownHook(shutdownHook);
		} catch (IllegalStateException ignored) {
			// JVM is already shutting down
		} catch (Throwable t) {
			LOG.warn("Failed to register shutdown hook.");
		}
	}

	@Override
	public <T extends Savepoint> String storeSavepoint(T savepoint) throws IOException {
		Preconditions.checkNotNull(savepoint, "Savepoint");

		synchronized (shutDownLock) {
			if (shutDown) {
				throw new IllegalStateException("Shut down");
			} else {
				String path = "jobmanager://savepoints/" + currentId.incrementAndGet();
				savepoints.put(path, savepoint);
				return path;
			}
		}
	}

	@Override
	public Savepoint loadSavepoint(String path) throws IOException {
		Preconditions.checkNotNull(path, "Path");

		Savepoint savepoint;
		synchronized (shutDownLock) {
			savepoint = savepoints.get(path);
		}

		if (savepoint != null) {
			return savepoint;
		} else {
			throw new IllegalArgumentException("Invalid path '" + path + "'.");
		}
	}

	@Override
	public void disposeSavepoint(String path, ClassLoader classLoader) throws Exception {
		Preconditions.checkNotNull(path, "Path");
		Preconditions.checkNotNull(classLoader, "Class loader");

		Savepoint savepoint;
		synchronized (shutDownLock) {
			savepoint = savepoints.remove(path);
		}

		if (savepoint != null) {
			savepoint.dispose(classLoader);
		} else {
			throw new IllegalArgumentException("Invalid path '" + path + "'.");
		}
	}

	@Override
	public void shutdown() throws Exception {
		synchronized (shutDownLock) {
			// This is problematic as the user code class loader is not
			// available at this point.
			for (Savepoint savepoint : savepoints.values()) {
				try {
					savepoint.dispose(ClassLoader.getSystemClassLoader());
				} catch (Throwable t) {
					LOG.warn("Failed to dispose savepoint " + savepoint.getCheckpointId(), t);
				}
			}

			savepoints.clear();

			// Remove shutdown hook to prevent resource leaks, unless this is
			// invoked by the shutdown hook itself.
			if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				} catch (IllegalStateException ignored) {
					// Race, JVM is in shutdown already, we can safely ignore this
				} catch (Throwable t) {
					LOG.warn("Failed to unregister shut down hook.");
				}
			}

			shutDown = true;
		}
	}

}
