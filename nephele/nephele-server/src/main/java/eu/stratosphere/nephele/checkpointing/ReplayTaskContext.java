/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.routing.InputGateContext;
import eu.stratosphere.nephele.taskmanager.routing.OutputGateContext;
import eu.stratosphere.nephele.taskmanager.routing.RoutingService;
import eu.stratosphere.nephele.taskmanager.routing.TaskContext;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTaskContext;

final class ReplayTaskContext implements TaskContext, BufferProvider, AsynchronousEventListener {

	private final ReplayTask task;

	private final RoutingService routingService;

	private final LocalBufferPoolOwner previousBufferPoolOwner;

	private final int numberOfChannels;

	private final LocalBufferPool localBufferPool;

	ReplayTaskContext(final ReplayTask task, final RoutingService routingService,
			final LocalBufferPoolOwner previousBufferPoolOwner, final int numberOfChannels) {
		this.task = task;
		this.routingService = routingService;
		this.previousBufferPoolOwner = previousBufferPoolOwner;
		if (previousBufferPoolOwner == null) {
			this.localBufferPool = new LocalBufferPool(1, false, this);
		} else {
			if (!(previousBufferPoolOwner instanceof RuntimeTaskContext)) {
				throw new IllegalStateException("previousBufferPoolOwner is not of type RuntimeTaskContext");
			}

			// Use the original task's buffer pool
			final RuntimeTaskContext rtc = (RuntimeTaskContext) previousBufferPoolOwner;
			this.localBufferPool = rtc.getLocalBufferPool();
		}
		this.numberOfChannels = numberOfChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGateContext createOutputGateContext(final GateID gateID) {

		return new ReplayOutputGateContext(this, gateID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGateContext createInputGateContext(final GateID gateID) {

		return new ReplayInputGateContext(gateID);
	}

	void registerReplayOutputBroker(final ChannelID channelID, final ReplayOutputChannelBroker outputBroker) {

		this.task.registerReplayOutputBroker(channelID, outputBroker);
	}

	RoutingService getRoutingService() {

		return this.routingService;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		return this.localBufferPool.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		return this.localBufferPool.requestEmptyBufferBlocking(minimumSizeOfBuffer);
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
	public boolean isShared() {

		return false;
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
	public int getMinimumNumberOfRequiredBuffers() {

		return this.numberOfChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setDesignatedNumberOfBuffers(final int numberOfBuffers) {

		this.localBufferPool.setDesignatedNumberOfBuffers(numberOfBuffers);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clearLocalBufferPool() {

		if (this.previousBufferPoolOwner != null) {
			// If the buffer pool was previously owned by the runtime task, let the runtime task do the clean-up
			this.previousBufferPoolOwner.clearLocalBufferPool();
		} else {
			// Otherwise, destroy the buffer pool yourself
			this.localBufferPool.destroy();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logBufferUtilization() {

		final int ava = this.localBufferPool.getNumberOfAvailableBuffers();
		final int req = this.localBufferPool.getRequestedNumberOfBuffers();
		final int des = this.localBufferPool.getDesignatedNumberOfBuffers();

		final Environment environment = this.task.getEnvironment();

		System.out.println("\t\t" + environment.getTaskName() + " (Replay): " + ava + " available, " + req
			+ " requested, " + des + " designated");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void asynchronousEventOccurred() throws IOException, InterruptedException {

		throw new IllegalStateException("ReplayTaskContext received asynchronous event");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {

		return this.localBufferPool.registerBufferAvailabilityListener(bufferAvailabilityListener);
	}
}
