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

package org.apache.flink.runtime.io.async;

import java.io.Closeable;
import java.io.IOException;

/**
 * The abstract class encapsulates the lifecycle and execution strategy for asynchronous IO operations
 *
 * @param <V> return type of the asynchronous call
 * @param <D> type of the IO handle
 */
public abstract class AbstractAsyncIOCallable<V, D extends Closeable> implements StoppableCallbackCallable<V> {

	private volatile boolean stopped;

	/**
	 * Closable handle to IO, e.g. an InputStream
	 */
	private volatile D ioHandle;

	/**
	 * Stores exception that might happen during close
	 */
	private volatile IOException stopException;


	public AbstractAsyncIOCallable() {
		this.stopped = false;
	}

	/**
	 * This method implements the strategy for the actual IO operation:
	 *
	 * 1) Open the IO handle
	 * 2) Perform IO operation
	 * 3) Close IO handle
	 *
	 * @return Result of the IO operation, e.g. a deserialized object.
	 * @throws Exception exception that happened during the call.
	 */
	@Override
	public V call() throws Exception {

		synchronized (this) {
			if (isStopped()) {
				throw new IOException("Task was already stopped. No I/O handle opened.");
			}

			ioHandle = openIOHandle();
		}

		try {

			return performOperation();

		} finally {
			closeIOHandle();
		}

	}

	/**
	 * Open the IO Handle (e.g. a stream) on which the operation will be performed.
	 *
	 * @return the opened IO handle that implements #Closeable
	 * @throws Exception
	 */
	protected abstract D openIOHandle() throws Exception;

	/**
	 * Implements the actual IO operation on the opened IO handle.
	 *
	 * @return Result of the IO operation
	 * @throws Exception
	 */
	protected abstract V performOperation() throws Exception;

	/**
	 * Stops the I/O operation by closing the I/O handle. If an exception is thrown on close, it can be accessed via
	 * #getStopException().
	 */
	@Override
	public void stop() {
		closeIOHandle();
	}

	private synchronized void closeIOHandle() {

		if (!stopped) {
			stopped = true;

			final D handle = ioHandle;
			if (handle != null) {
				try {
					handle.close();
				} catch (IOException ex) {
					stopException = ex;
				}
			}
		}
	}

	/**
	 * Returns the IO handle.
	 * @return the IO handle
	 */
	protected D getIoHandle() {
		return ioHandle;
	}

	/**
	 * Optional callback that subclasses can implement. This is called when the callable method completed, e.g. because
	 * it finished or was stopped.
	 */
	@Override
	public void done(boolean canceled) {
		//optional callback hook
	}

	/**
	 * Check if the IO operation is stopped
	 *
	 * @return true if stop() was called
	 */
	@Override
	public boolean isStopped() {
		return stopped;
	}

	/**
	 * Returns Exception that might happen on stop.
	 *
	 * @return Potential Exception that happened open stopping.
	 */
	@Override
	public IOException getStopException() {
		return stopException;
	}
}
