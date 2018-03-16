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

import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

/**
 * This abstract class encapsulates the lifecycle and execution strategy for asynchronous operations that use resources.
 *
 * @param <V> return type of the asynchronous call.
 */
public abstract class AbstractAsyncCallableWithResources<V> implements StoppableCallbackCallable<V> {

	/** Tracks if the stop method was called on this object. */
	private volatile boolean stopped;

	/** Tracks if call method was executed (only before stop calls). */
	private volatile boolean called;

	/** Stores a collected exception if there was one during stop. */
	private volatile Exception stopException;

	public AbstractAsyncCallableWithResources() {
		this.stopped = false;
		this.called = false;
	}

	/**
	 * This method implements the strategy for the actual IO operation:
	 * <p>
	 * 1) Acquire resources asynchronously and atomically w.r.t stopping.
	 * 2) Performs the operation
	 * 3) Releases resources.
	 *
	 * @return Result of the IO operation, e.g. a deserialized object.
	 * @throws Exception exception that happened during the call.
	 */
	@Override
	public final V call() throws Exception {

		V result = null;
		Exception collectedException = null;

		try {
			synchronized (this) {

				if (stopped) {
					throw new IOException("Task was already stopped.");
				}

				called = true;
				// Get resources in async part, atomically w.r.t. stopping.
				acquireResources();
			}

			// The main work is performed here.
			result = performOperation();

		} catch (Exception ex) {
			collectedException = ex;
		} finally {

			try {
				// Cleanup
				releaseResources();
			} catch (Exception relEx) {
				collectedException = ExceptionUtils.firstOrSuppressed(relEx, collectedException);
			}

			if (collectedException != null) {
				throw collectedException;
			}
		}

		return result;
	}

	/**
	 * Open the IO Handle (e.g. a stream) on which the operation will be performed.
	 *
	 * @return the opened IO handle that implements #Closeable
	 * @throws Exception if there was a problem in acquiring.
	 */
	protected abstract void acquireResources() throws Exception;

	/**
	 * Implements the actual operation.
	 *
	 * @return Result of the operation
	 * @throws Exception if there was a problem in executing the operation.
	 */
	protected abstract V performOperation() throws Exception;

	/**
	 * Releases resources acquired by this object.
	 *
	 * @throws Exception if there was a problem in releasing resources.
	 */
	protected abstract void releaseResources() throws Exception;

	/**
	 * This method implements how the operation is stopped. Usually this involves interrupting or closing some
	 * resources like streams to return from blocking calls.
	 *
	 * @throws Exception on problems during the stopping.
	 */
	protected abstract void stopOperation() throws Exception;

	/**
	 * Stops the I/O operation by closing the I/O handle. If an exception is thrown on close, it can be accessed via
	 * #getStopException().
	 */
	@Override
	public final void stop() {

		synchronized (this) {

			// Make sure that call can not enter execution from here.
			if (stopped) {
				return;
			} else {
				stopped = true;
			}
		}

		if (called) {
			// Async call is executing -> attempt to stop it and releaseResources() will happen inside the async method.
			try {
				stopOperation();
			} catch (Exception stpEx) {
				this.stopException = stpEx;
			}
		} else {
			// Async call was not executed, so we also need to releaseResources() here.
			try {
				releaseResources();
			} catch (Exception relEx) {
				stopException = relEx;
			}
		}
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
	 * True once the async method was called.
	 */
	public boolean isCalled() {
		return called;
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
	 * Returns a potential exception that might have been observed while stopping the operation.
	 */
	@Override
	public Exception getStopException() {
		return stopException;
	}
}
