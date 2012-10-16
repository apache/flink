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
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * This class provides an abstract base class for an output gate wrapper. An output gate wrapper can be used by a plugin
 * to wrap an output gate and intercept particular method calls. The default implementation of this abstract base class
 * simply forwards every method call to the encapsulated output gate.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record transported through this output gate
 */
public abstract class AbstractOutputGateWrapper<T extends Record> implements OutputGate<T> {

	/**
	 * The wrapped output gate.
	 */
	private final OutputGate<T> wrappedOutputGate;

	/**
	 * Constructs a new abstract output gate wrapper.
	 * 
	 * @param wrappedOutputGate
	 *        the output gate to be wrapped
	 */
	public AbstractOutputGateWrapper(final OutputGate<T> wrappedOutputGate) {

		if (wrappedOutputGate == null) {
			throw new IllegalArgumentException("Argument wrappedOutputGate must not be null");
		}

		this.wrappedOutputGate = wrappedOutputGate;
	}

	/**
	 * Returns the wrapped output gate.
	 * 
	 * @return the wrapped output gate
	 */
	protected OutputGate<T> getWrappedOutputGate() {

		return this.wrappedOutputGate;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndex() {

		return this.wrappedOutputGate.getIndex();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribeToEvent(final EventListener eventListener, final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedOutputGate.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unsubscribeFromEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		this.wrappedOutputGate.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(final AbstractTaskEvent event) throws IOException, InterruptedException {

		this.wrappedOutputGate.publishEvent(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deliverEvent(final AbstractTaskEvent event) {

		this.wrappedOutputGate.deliverEvent(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.wrappedOutputGate.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getChannelType() {

		return this.wrappedOutputGate.getChannelType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CompressionLevel getCompressionLevel() {

		return this.wrappedOutputGate.getCompressionLevel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getGateID() {

		return this.wrappedOutputGate.getGateID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		this.wrappedOutputGate.releaseAllChannelResources();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		return this.wrappedOutputGate.isClosed();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return this.wrappedOutputGate.isInputGate();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException, InterruptedException {

		this.wrappedOutputGate.writeRecord(record);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {

		this.wrappedOutputGate.flush();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBroadcast() {

		return this.wrappedOutputGate.isBroadcast();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelSelector<T> getChannelSelector() {

		return this.wrappedOutputGate.getChannelSelector();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestClose() throws IOException, InterruptedException {

		this.wrappedOutputGate.requestClose();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeAllOutputChannels() {

		this.wrappedOutputGate.removeAllOutputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createNetworkOutputChannel(final OutputGate<T> outputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		this.wrappedOutputGate.createNetworkOutputChannel(outputGate, channelID, connectedChannelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createFileOutputChannel(final OutputGate<T> outputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		this.wrappedOutputGate.createFileOutputChannel(outputGate, channelID, connectedChannelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createInMemoryOutputChannel(final OutputGate<T> outputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		this.wrappedOutputGate.createInMemoryOutputChannel(outputGate, channelID, connectedChannelID, compressionLevel);
	}
}
