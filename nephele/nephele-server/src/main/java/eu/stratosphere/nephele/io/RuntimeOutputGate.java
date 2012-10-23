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

package eu.stratosphere.nephele.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.FileOutputChannel;
import eu.stratosphere.nephele.io.channels.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.channels.NetworkOutputChannel;
import eu.stratosphere.nephele.io.channels.RecordSerializerFactory;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * In Nephele output gates are a specialization of general gates and connect
 * record writers and output channels. As channels, output gates are always
 * parameterized to a specific type of record which they can transport.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public class RuntimeOutputGate<T extends Record> extends AbstractGate<T> implements OutputGate<T> {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(OutputGate.class);

	/**
	 * Stores whether all records passed to this output gate shall be transmitted through all connected output channels.
	 */
	private final boolean isBroadcast;

	/**
	 * The factory used to instantiate new records serializers.
	 */
	private final RecordSerializerFactory<T> serializerFactory;

	/**
	 * Channel selector to determine which channel is supposed receive the next record.
	 */
	private final ChannelSelector<T> channelSelector;

	/**
	 * The list of output channels attached to this gate.
	 */
	private final ArrayList<AbstractOutputChannel<T>> outputChannels = new ArrayList<AbstractOutputChannel<T>>();

	/**
	 * The thread which executes the task connected to the output gate.
	 */
	private Thread executingThread = null;

	/**
	 * Constructs a new runtime output gate.
	 * 
	 * @param jobID
	 *        the ID of the job this input gate belongs to
	 * @param gateID
	 *        the ID of the gate
	 * @param index
	 *        the index assigned to this output gate at the {@link Environment} object
	 * @param channelType
	 *        the type of the channels connected to this output gate
	 * @param compressionLevel
	 *        the compression level of the channels which are connected to this output gate
	 * @param channelSelector
	 *        the channel selector to be used for this output gate
	 * @param isBroadcast
	 *        <code>true</code> if every records passed to this output gate shall be transmitted through all connected
	 *        output channels, <code>false</code> otherwise
	 * @param serializerFactory
	 *        the factory for the record serializer
	 */
	public RuntimeOutputGate(final JobID jobID, final GateID gateID, final int index, final ChannelType channelType,
			final CompressionLevel compressionLevel, final ChannelSelector<T> channelSelector,
			final boolean isBroadcast, final RecordSerializerFactory<T> serializerFactory) {

		super(jobID, gateID, index, channelType, compressionLevel);

		this.isBroadcast = isBroadcast;
		this.serializerFactory = serializerFactory;

		if (this.isBroadcast) {
			this.channelSelector = null;
		} else {
			if (channelSelector == null) {
				this.channelSelector = new DefaultChannelSelector<T>();
			} else {
				this.channelSelector = channelSelector;
			}
		}
	}

	/**
	 * Adds a new output channel to the output gate.
	 * 
	 * @param outputChannel
	 *        the output channel to be added.
	 */
	private void addOutputChannel(AbstractOutputChannel<T> outputChannel) {
		if (!this.outputChannels.contains(outputChannel)) {
			this.outputChannels.add(outputChannel);
		}
	}

	/**
	 * Removes the output channel with the given ID from the output gate if it
	 * exists.
	 * 
	 * @param outputChannelID
	 *        the ID of the channel to be removed
	 */
	public void removeOutputChannel(ChannelID outputChannelID) {

		for (int i = 0; i < this.outputChannels.size(); i++) {

			final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(i);
			if (outputChannel.getID().equals(outputChannelID)) {
				this.outputChannels.remove(i);
				return;
			}
		}

		LOG.debug("Cannot find output channel with ID " + outputChannelID + " to remove");
	}

	/**
	 * Removes all output channels from the output gate.
	 */
	public void removeAllOutputChannels() {

		this.outputChannels.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createNetworkOutputChannel(final OutputGate<T> outputGate,
			final ChannelID channelID, final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		final NetworkOutputChannel<T> enoc = new NetworkOutputChannel<T>(outputGate, this.outputChannels.size(),
			channelID, connectedChannelID, compressionLevel, this.serializerFactory.createSerializer());
		addOutputChannel(enoc);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createFileOutputChannel(final OutputGate<T> outputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		final FileOutputChannel<T> efoc = new FileOutputChannel<T>(outputGate, this.outputChannels.size(), channelID,
			connectedChannelID, compressionLevel, this.serializerFactory.createSerializer());
		addOutputChannel(efoc);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createInMemoryOutputChannel(final OutputGate<T> outputGate,
			final ChannelID channelID, final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		final InMemoryOutputChannel<T> einoc = new InMemoryOutputChannel<T>(outputGate, this.outputChannels.size(),
			channelID, connectedChannelID, compressionLevel, this.serializerFactory.createSerializer());
		addOutputChannel(einoc);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestClose() throws IOException, InterruptedException {
		// Close all output channels
		for (int i = 0; i < this.outputChannels.size(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(i);
			outputChannel.requestClose();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		boolean allClosed = true;

		for (int i = 0; i < this.outputChannels.size(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(i);
			if (!outputChannel.isClosed()) {
				allClosed = false;
			}
		}

		return allClosed;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException, InterruptedException {

		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		if (this.isBroadcast) {

			if (getChannelType() == ChannelType.INMEMORY) {

				final int numberOfOutputChannels = this.outputChannels.size();
				for (int i = 0; i < numberOfOutputChannels; ++i) {
					this.outputChannels.get(i).writeRecord(record);
				}

			} else {

				// Use optimization for byte buffered channels
				this.outputChannels.get(0).writeRecord(record);
			}

		} else {

			// Non-broadcast gate, use channel selector to select output channels
			final int numberOfOutputChannels = this.outputChannels.size();
			final int[] selectedOutputChannels = this.channelSelector.selectChannels(record, numberOfOutputChannels);

			if (selectedOutputChannels == null) {
				return;
			}

			for (int i = 0; i < selectedOutputChannels.length; ++i) {

				if (selectedOutputChannels[i] < numberOfOutputChannels) {
					final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(selectedOutputChannels[i]);
					outputChannel.writeRecord(record);
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Output " + super.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();
		while (it.hasNext()) {
			it.next().transferEvent(event);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {
		// Flush all connected channels
		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();
		while (it.hasNext()) {
			it.next().flush();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBroadcast() {

		return this.isBroadcast;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelSelector<T> getChannelSelector() {

		return this.channelSelector;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();

		while (it.hasNext()) {
			it.next().releaseAllResources();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initializeCompressors() throws CompressionException {

		// In-memory channels to not use compression, so there is nothing to initialize
		if (getChannelType() == ChannelType.INMEMORY) {
			return;
		}

		// Check if this channel is configured to use compression
		if (getCompressionLevel() == CompressionLevel.NO_COMPRESSION) {
			return;
		}

		for (int i = 0; i < this.outputChannels.size(); ++i) {
			this.outputChannels.get(i).initializeCompressor();
		}
	}

	/**
	 * Returns the number of output channels associated with this output gate.
	 * 
	 * @return the number of output channels associated with this output gate
	 */
	public int getNumberOfOutputChannels() {

		return this.outputChannels.size();
	}

	/**
	 * Returns the output channel from position <code>pos</code> of the gate's internal channel list.
	 * 
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such position does not exist.
	 */
	public AbstractOutputChannel<T> getOutputChannel(final int pos) {

		if (pos < this.outputChannels.size()) {
			return this.outputChannels.get(pos);
		}

		return null;
	}
}
