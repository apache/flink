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

package eu.stratosphere.nephele.plugins.wrapper;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * This class provides an abstract base class for an input gate wrapper. An input gate wrapper can be used by a plugin
 * to wrap an input gate and intercept particular method calls. The default implementation of this abstract base class
 * simply forwards every method call to the encapsulated input gate.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record transported through this input gate
 */
public abstract class AbstractInputGateWrapper<T extends Record> implements InputGate<T> {

	/**
	 * The wrapped input gate.
	 */
	private final InputGate<T> wrappedInputGate;

	/**
	 * Constructs a new abstract input gate wrapper.
	 * 
	 * @param wrappedInputGate
	 *        the input gate to be wrapped
	 */
	public AbstractInputGateWrapper(final InputGate<T> wrappedInputGate) {

		if (wrappedInputGate == null) {
			throw new IllegalArgumentException("Argument wrappedInputGate must not be null");
		}

		this.wrappedInputGate = wrappedInputGate;
	}

	/**
	 * Returns the wrapped input gate.
	 * 
	 * @return the wrapped input gate
	 */
	protected InputGate<T> getWrappedInputGate() {

		return this.wrappedInputGate;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndex() {

		return this.wrappedInputGate.getIndex();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribeToEvent(final EventListener eventListener, final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedInputGate.subscribeToEvent(eventListener, eventType);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unsubscribeFromEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedInputGate.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(final AbstractTaskEvent event) throws IOException, InterruptedException {

		this.wrappedInputGate.publishEvent(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deliverEvent(AbstractTaskEvent event) {

		this.wrappedInputGate.deliverEvent(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.wrappedInputGate.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getChannelType() {

		return this.wrappedInputGate.getChannelType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompressionLevel getCompressionLevel() {

		return this.wrappedInputGate.getCompressionLevel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getGateID() {

		return this.wrappedInputGate.getGateID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		this.wrappedInputGate.releaseAllChannelResources();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		return this.wrappedInputGate.isClosed();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return this.wrappedInputGate.isInputGate();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord(final T target) throws IOException, InterruptedException {

		return this.wrappedInputGate.readRecord(target);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(final int channelIndex) {

		this.wrappedInputGate.notifyRecordIsAvailable(channelIndex);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void activateInputChannels() throws IOException, InterruptedException {

		this.wrappedInputGate.activateInputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException, InterruptedException {

		this.wrappedInputGate.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createNetworkInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		this.wrappedInputGate.createNetworkInputChannel(inputGate, channelID, connectedChannelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createFileInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		this.wrappedInputGate.createFileInputChannel(inputGate, channelID, connectedChannelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createInMemoryInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		this.wrappedInputGate.createInMemoryInputChannel(inputGate, channelID, connectedChannelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeAllInputChannels() {

		this.wrappedInputGate.removeAllInputChannels();
	}
}
