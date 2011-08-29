/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;

final class TaskContext implements BufferProvider, LocalBufferPoolOwner, AsynchronousEventListener {

	private final LocalBufferPool localBufferPool;

	private final Environment environment;

	private final AsynchronousEventListener[] subEventListener;

	private final int numberOfOutputChannels;

	/**
	 * Stores whether the initial exhaustion of memory buffers has already been reported
	 */
	private boolean initialExhaustionOfMemoryBuffersReported = false;

	TaskContext(final Environment environment) {

		this.localBufferPool = new LocalBufferPool(1, false, this);

		this.environment = environment;

		// Compute number of input input channels
		int nooc = 0;
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			nooc += environment.getOutputGate(i).getNumberOfOutputChannels();
		}
		this.numberOfOutputChannels = nooc; 

		// Each output gate context will register as a sub event listener
		this.subEventListener = new AsynchronousEventListener[environment.getNumberOfOutputGates()];
	}

	void registerAsynchronousEventListener(final int index, final AsynchronousEventListener eventListener) {

		if (index >= this.subEventListener.length || index < 0) {
			throw new IllegalArgumentException("Argument index has invalid value " + index);
		}

		if (eventListener == null) {
			throw new IllegalArgumentException("Argument eventListener must not be null");
		}

		if (this.subEventListener[index] != null) {
			throw new IllegalStateException("There is already an event listener with index " + index + " registered");
		}

		this.subEventListener[index] = eventListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer, final int minimumReserve) throws IOException {

		return this.localBufferPool.requestEmptyBuffer(minimumSizeOfBuffer, minimumReserve);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer, final int minimumReserve) throws IOException,
			InterruptedException {

		return this.localBufferPool.requestEmptyBufferBlocking(minimumSizeOfBuffer, minimumReserve);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.localBufferPool.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clearLocalBufferPool() {

		// Clear the buffer cache
		this.localBufferPool.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logBufferUtilization() {

		final int ava = this.localBufferPool.getNumberOfAvailableBuffers();
		final int req = this.localBufferPool.getRequestedNumberOfBuffers();
		final int des = this.localBufferPool.getDesignatedNumberOfBuffers();

		System.out.println("\t\t" + this.environment.getTaskName() + ": " + ava + " available, " + req + " requested, "
			+ des + " designated");
	}

	/**
	 * Called by an {@link OutputGateContext} to indicate that the task has temporarily run out of memory buffers.
	 */
	void reportExhaustionOfMemoryBuffers() {

		if (!this.initialExhaustionOfMemoryBuffersReported) {

			this.environment.triggerInitialExecutionResourcesExhaustedNotification();
			this.initialExhaustionOfMemoryBuffersReported = true;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.localBufferPool.reportAsynchronousEvent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void asynchronousEventOccurred() throws IOException, InterruptedException {

		for (int i = 0; i < this.subEventListener.length; ++i) {

			if (this.subEventListener[i] == null) {
				throw new IllegalStateException("Event listener at index " + i + " is null");
			}

			this.subEventListener[i].asynchronousEventOccurred();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfChannels() {
		
		return this.numberOfOutputChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setDesignatedNumberOfBuffers(int numberOfBuffers) {

		this.localBufferPool.setDesignatedNumberOfBuffers(numberOfBuffers);
	}
}
