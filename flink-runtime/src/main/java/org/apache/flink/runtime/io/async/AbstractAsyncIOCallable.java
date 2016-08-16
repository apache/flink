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

import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @param <V> return type of the asynchronous call
 * @param <D> type of the IO handle
 */
public abstract class AbstractAsyncIOCallable<V, D extends Closeable> implements StoppableCallbackCallable {

	// Closable handle to IO, e.g. an InputStream
	protected final D ioHandle;

	// Flag that is true if stop() was called
	protected boolean stopCalled;

	// Saves IOException that might happen on closing the IO handle
	protected IOException closeException;

	/**
	 *
	 * @param ioHandle typically a stream to perform IO operations on
	 */
	public AbstractAsyncIOCallable(D ioHandle) {
		this.ioHandle = Preconditions.checkNotNull(ioHandle);
		this.stopCalled = false;
		this.closeException = null;
	}

	/**
	 * This method implements the actual IO operation that is performed asynchronously.
	 *
	 * @return Result of the IO operation, e.g. a deserialized object.
	 * @throws Exception exception that happened during the call.
	 */
	@Override
	public abstract V call() throws Exception;

	/**
	 * Stops the IO operation by closing the IO handle. If an exception is thrown from closing the
	 */
	@Override
	public void stop() {
		if (!stopCalled) {
			stopCalled = true;
			try {
				ioHandle.close();
			} catch (IOException ex) {
				closeException = ex;
			}
		}
	}

	/**
	 * @return this returns the exception that might have been triggered on closing the IO handle, or null if no
	 * exception occurred.
	 */
	public IOException getCloseException() {
		return closeException;
	}

	/**
	 *
	 * @return the ioHandle
	 */
	protected D getIoHandle() {
		return ioHandle;
	}

	/**
	 * Optional callback that subclasses can implement. This is called when the callable method completed, e.g. because
	 * it finished or was stopped.
	 */
	@Override
	public void done() {
		//optional callback hook
	}

	/**
	 *
	 * @return true if stop() was called
	 */
	public boolean isStopCalled() {
		return stopCalled;
	}
}
