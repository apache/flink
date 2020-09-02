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

package org.apache.flink.runtime.operators.sort;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all working threads in this sort-merger. The specific threads for reading, sorting, spilling,
 * merging, etc... extend this subclass.
 * <p>
 * The threads are designed to terminate themselves when the task they are set up to do is completed. Further more,
 * they terminate immediately when the <code>shutdown()</code> method is called.
 */
abstract class ThreadBase<E> extends Thread implements Thread.UncaughtExceptionHandler, StageRunner {

	/**
	 * The queue of empty buffer that can be used for reading;
	 */
	protected final StageMessageDispatcher<E> dispatcher;

	/**
	 * The exception handler for any problems.
	 */
	private final ExceptionHandler<IOException> exceptionHandler;

	/**
	 * The flag marking this thread as alive.
	 */
	private volatile boolean alive;

	/**
	 * Creates a new thread.
	 *
	 * @param exceptionHandler The exception handler to call for all exceptions.
	 * @param name The name of the thread.
	 * @param queues The queues used to pass buffers between the threads.
	 */
	protected ThreadBase(
			@Nullable ExceptionHandler<IOException> exceptionHandler,
			String name,
			StageMessageDispatcher<E> queues) {
		// thread setup
		super(checkNotNull(name));
		this.setDaemon(true);

		// exception handling
		this.exceptionHandler = exceptionHandler;
		this.setUncaughtExceptionHandler(this);

		this.dispatcher = checkNotNull(queues);
		this.alive = true;
	}

	/**
	 * Implements exception handling and delegates to go().
	 */
	public void run() {
		try {
			go();
		}
		catch (Throwable t) {
			internalHandleException(new IOException("Thread '" + getName() + "' terminated due to an exception: "
				+ t.getMessage(), t));
		}
	}

	/**
	 * Equivalent to the run() method.
	 *
	 * @throws IOException Exceptions that prohibit correct completion of the work may be thrown by the thread.
	 */
	protected abstract void go() throws IOException, InterruptedException;

	/**
	 * Checks whether this thread is still alive.
	 *
	 * @return true, if the thread is alive, false otherwise.
	 */
	protected boolean isRunning() {
		return this.alive;
	}

	@Override
	public void close() throws InterruptedException {
		this.alive = false;
		this.interrupt();
		this.join();
	}

	/**
	 * Internally handles an exception and makes sure that this method returns without a problem.
	 *
	 * @param ioex
	 *        The exception to handle.
	 */
	protected final void internalHandleException(IOException ioex) {
		if (!isRunning()) {
			// discard any exception that occurs when after the thread is killed.
			return;
		}
		if (this.exceptionHandler != null) {
			try {
				this.exceptionHandler.handleException(ioex);
			}
			catch (Throwable ignored) {}
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		internalHandleException(new IOException("Thread '" + t.getName()
			+ "' terminated due to an uncaught exception: " + e.getMessage(), e));
	}
}
