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

package org.apache.flink.runtime.state;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A simple base for closable handles.
 * 
 * Offers to register a stream (or other closable object) that close calls are delegated to if
 * the handle is closed or was already closed.
 */
public abstract class AbstractCloseableHandle implements Closeable, Serializable {

	/** Serial Version UID must be constant to maintain format compatibility */
	private static final long serialVersionUID = 1L;

	/** To atomically update the "closable" field without needing to add a member class like "AtomicBoolean */
	private static final AtomicIntegerFieldUpdater<AbstractCloseableHandle> CLOSER = 
			AtomicIntegerFieldUpdater.newUpdater(AbstractCloseableHandle.class, "isClosed");

	// ------------------------------------------------------------------------

	/** The closeable to close if this handle is closed late */ 
	private transient volatile Closeable toClose;

	/** Flag to remember if this handle was already closed */
	@SuppressWarnings("unused") // this field is actually updated, but via the "CLOSER" updater
	private transient volatile int isClosed;

	// ------------------------------------------------------------------------

	protected final void registerCloseable(Closeable toClose) throws IOException {
		if (toClose == null) {
			return;
		}
		
		// NOTE: The order of operations matters here:
		// (1) first setting the closeable
		// (2) checking the flag.
		// Because the order in the {@link #close()} method is the opposite, and
		// both variables are volatile (reordering barriers), we can be sure that
		// one of the methods always notices the effect of a concurrent call to the
		// other method.

		this.toClose = toClose;

		// check if we were closed early
		if (this.isClosed != 0) {
			toClose.close();
			throw new IOException("handle is closed");
		}
	}

	/**
	 * Closes the handle.
	 * 
	 * <p>If a "Closeable" has been registered via {@link #registerCloseable(Closeable)},
	 * then this will be closes.
	 * 
	 * <p>If any "Closeable" will be registered via {@link #registerCloseable(Closeable)} in the future,
	 * it will immediately be closed and that method will throw an exception.
	 * 
	 * @throws IOException Exceptions occurring while closing an already registered {@code Closeable}
	 *                     are forwarded.
	 * 
	 * @see #registerCloseable(Closeable)
	 */
	@Override
	public final void close() throws IOException {
		// NOTE: The order of operations matters here:
		// (1) first setting the closed flag
		// (2) checking whether there is already a closeable
		// Because the order in the {@link #registerCloseable(Closeable)} method is the opposite, and
		// both variables are volatile (reordering barriers), we can be sure that
		// one of the methods always notices the effect of a concurrent call to the
		// other method.

		if (CLOSER.compareAndSet(this, 0, 1)) {
			final Closeable toClose = this.toClose;
			if (toClose != null) {
				this.toClose = null;
				toClose.close();
			}
		}
	}

	/**
	 * Checks whether this handle has been closed.
	 * 
	 * @return True is the handle is closed, false otherwise.
	 */
	public boolean isClosed() {
		return isClosed != 0;
	}

	/**
	 * This method checks whether the handle is closed and throws an exception if it is closed.
	 * If the handle is not closed, this method does nothing.
	 * 
	 * @throws IOException Thrown, if the handle has been closed.
	 */
	public void ensureNotClosed() throws IOException {
		if (isClosed != 0) {
			throw new IOException("handle is closed");
		}
	}
}
