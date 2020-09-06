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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.CloseableRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class that outlines the strategy for asynchronous snapshots. Implementations of this class are typically
 * instantiated with resources that have been created in the synchronous part of a snapshot. Then, the implementation
 * of {@link #callInternal()} is invoked in the asynchronous part. All resources created by this methods should
 * be released by the end of the method. If the created resources are {@link Closeable} objects and can block in calls
 * (e.g. in/output streams), they should be registered with the snapshot's {@link CloseableRegistry} so that the can
 * be closed and unblocked on cancellation. After {@link #callInternal()} ended, {@link #logAsyncSnapshotComplete(long)}
 * is called. In that method, implementations can emit log statements about the duration. At the very end, this class
 * calls {@link #cleanupProvidedResources()}. The implementation of this method should release all provided resources
 * that have been passed into the snapshot from the synchronous part of the snapshot.
 *
 * @param <T> type of the result.
 */
public abstract class AsyncSnapshotCallable<T> implements Callable<T> {

	/** Message for the {@link CancellationException}. */
	private static final String CANCELLATION_EXCEPTION_MSG = "Async snapshot was cancelled.";

	private static final Logger LOG = LoggerFactory.getLogger(AsyncSnapshotCallable.class);

	/** This is used to atomically claim ownership for the resource cleanup. */
	@Nonnull
	private final AtomicBoolean resourceCleanupOwnershipTaken;

	/** Registers streams that can block in I/O during snapshot. Forwards close from taskCancelCloseableRegistry. */
	@Nonnull
	protected final CloseableRegistry snapshotCloseableRegistry;

	protected AsyncSnapshotCallable() {
		this.snapshotCloseableRegistry = new CloseableRegistry();
		this.resourceCleanupOwnershipTaken = new AtomicBoolean(false);
	}

	@Override
	public T call() throws Exception {
		final long startTime = System.currentTimeMillis();

		if (resourceCleanupOwnershipTaken.compareAndSet(false, true)) {
			try {
				T result = callInternal();
				logAsyncSnapshotComplete(startTime);
				return result;
			} catch (Exception ex) {
				if (!snapshotCloseableRegistry.isClosed()) {
					throw ex;
				}
			} finally {
				closeSnapshotIO();
				cleanup();
			}
		}

		throw new CancellationException(CANCELLATION_EXCEPTION_MSG);
	}

	@VisibleForTesting
	protected void cancel() {
		closeSnapshotIO();
		if (resourceCleanupOwnershipTaken.compareAndSet(false, true)) {
			cleanup();
		}
	}

	/**
	 * Creates a future task from this and registers it with the given {@link CloseableRegistry}. The task is
	 * unregistered again in {@link FutureTask#done()}.
	 */
	public AsyncSnapshotTask toAsyncSnapshotFutureTask(@Nonnull CloseableRegistry taskRegistry) throws IOException {
		return new AsyncSnapshotTask(taskRegistry);
	}

	/**
	 * {@link FutureTask} that wraps a {@link AsyncSnapshotCallable} and connects it with cancellation and closing.
	 */
	public class AsyncSnapshotTask extends FutureTask<T> {

		@Nonnull
		private final CloseableRegistry taskRegistry;

		@Nonnull
		private final Closeable cancelOnClose;

		private AsyncSnapshotTask(@Nonnull CloseableRegistry taskRegistry) throws IOException {
			super(AsyncSnapshotCallable.this);
			this.cancelOnClose = () -> cancel(true);
			this.taskRegistry = taskRegistry;
			taskRegistry.registerCloseable(cancelOnClose);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			boolean result = super.cancel(mayInterruptIfRunning);
			if (mayInterruptIfRunning) {
				AsyncSnapshotCallable.this.cancel();
			}
			return result;
		}

		@Override
		protected void done() {
			super.done();
			taskRegistry.unregisterCloseable(cancelOnClose);
		}
	}

	/**
	 * This method implements the (async) snapshot logic. Resources aquired within this method should be released at
	 * the end of the method.
	 */
	protected abstract T callInternal() throws Exception;

	/**
	 * This method implements the cleanup of resources that have been passed in (from the sync part). Called after the
	 * end of {@link #callInternal()}.
	 */
	protected abstract void cleanupProvidedResources();

	/**
	 * This method is invoked after completion of the snapshot and can be overridden to output a logging about the
	 * duration of the async part.
	 */
	protected void logAsyncSnapshotComplete(long startTime) {

	}

	private void cleanup() {
		cleanupProvidedResources();
	}

	private void closeSnapshotIO() {
		try {
			snapshotCloseableRegistry.close();
		} catch (IOException e) {
			LOG.warn("Could not properly close incremental snapshot streams.", e);
		}
	}
}
