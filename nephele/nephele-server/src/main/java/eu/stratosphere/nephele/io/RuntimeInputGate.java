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

package eu.stratosphere.nephele.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.FileInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * In Nephele input gates are a specialization of general gates and connect input channels and record readers. As
 * channels, input
 * gates are always parameterized to a specific type of record which they can transport. In contrast to output gates
 * input gates
 * can be associated with a {@link DistributionPattern} object which dictates the concrete wiring between two groups of
 * vertices.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public class RuntimeInputGate<T extends Record> extends AbstractGate<T> implements InputGate<T> {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(InputGate.class);

	/**
	 * The deserializer used to construct records from byte streams.
	 */
	private final RecordDeserializer<T> deserializer;

	/**
	 * The list of input channels attached to this input gate.
	 */
	private final ArrayList<AbstractInputChannel<T>> inputChannels = new ArrayList<AbstractInputChannel<T>>();

	/**
	 * The distribution pattern to determine how to wire the channels.
	 */
	private final DistributionPattern distributionPattern;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<Integer> availableChannels = new ArrayDeque<Integer>();

	/**
	 * The listener objects registered for this input gate.
	 */
	private InputGateListener[] inputGateListeners = null;

	/**
	 * The listener object to be notified when a channel has at least one record available.
	 */
	private RecordAvailabilityListener<T> recordAvailabilityListener = null;

	/**
	 * If the value of this variable is set to <code>true</code>, the input gate is closed.
	 */
	private boolean isClosed = false;

	/**
	 * The channel to read from next.
	 */
	private int channelToReadFrom = -1;

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	/**
	 * The Systemtime at the first arrival of a record
	 */
	private long executionstart = -1;

	private int numrecords = 0;
	/**
	 * Constructs a new runtime input gate.
	 * 
	 * @param jobID
	 *        the ID of the job this input gate belongs to
	 * @param gateID
	 *        the ID of the gate
	 * @param inputClass
	 *        the class of the record that can be transported through this gate
	 * @param index
	 *        the index assigned to this input gate at the {@link Environment} object
	 * @param distributionPattern
	 *        the distribution pattern to determine the concrete wiring between to groups of vertices
	 */
	public RuntimeInputGate(final JobID jobID, final GateID gateID, final RecordDeserializer<T> deserializer,
			final int index, final DistributionPattern distributionPattern) {

		super(jobID, gateID, index);

		this.deserializer = deserializer;

		this.distributionPattern = (distributionPattern != null) ? distributionPattern
			: new BipartiteDistributionPattern();
	}

	/**
	 * Adds a new input channel to the input gate.
	 * 
	 * @param inputChannel
	 *        the input channel to be added.
	 */
	private void addInputChannel(AbstractInputChannel<T> inputChannel) {

		if (!this.inputChannels.contains(inputChannel)) {
			this.inputChannels.add(inputChannel);
		}
	}

	/**
	 * Removes the input channel with the given ID from the input gate if it exists.
	 * 
	 * @param inputChannelID
	 *        the ID of the channel to be removed
	 */
	public void removeInputChannel(ChannelID inputChannelID) {

		for (int i = 0; i < this.inputChannels.size(); i++) {

			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			if (inputChannel.getID().equals(inputChannelID)) {
				this.inputChannels.remove(i);
				return;
			}
		}

		LOG.debug("Cannot find output channel with ID " + inputChannelID + " to remove");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeAllInputChannels() {

		this.inputChannels.clear();
	}

	public AbstractInputChannel<T> replaceChannel(ChannelID oldChannelID, ChannelType newChannelType) {

		AbstractInputChannel<T> oldInputChannel = null;

		for (int i = 0; i < this.inputChannels.size(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			if (inputChannel.getID().equals(oldChannelID)) {
				oldInputChannel = inputChannel;
				break;
			}
		}

		if (oldInputChannel == null) {
			return null;
		}

		AbstractInputChannel<T> newInputChannel = null;

		switch (newChannelType) {
		case FILE:
			newInputChannel = new FileInputChannel<T>(this, oldInputChannel.getChannelIndex(), deserializer,
					oldInputChannel.getID(), oldInputChannel.getCompressionLevel());
			break;
		case INMEMORY:
			newInputChannel = new InMemoryInputChannel<T>(this, oldInputChannel.getChannelIndex(), deserializer,
					oldInputChannel.getID(), oldInputChannel.getCompressionLevel());
			break;
		case NETWORK:
			newInputChannel = new NetworkInputChannel<T>(this, oldInputChannel.getChannelIndex(), deserializer,
					oldInputChannel.getID(), oldInputChannel.getCompressionLevel());
			break;
		default:
			LOG.error("Unknown input channel type");
			return null;
		}

		newInputChannel.setConnectedChannelID(oldInputChannel.getConnectedChannelID());

		this.inputChannels.set(newInputChannel.getChannelIndex(), newInputChannel);

		return newInputChannel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DistributionPattern getDistributionPattern() {
		return this.distributionPattern;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputChannels() {

		return this.inputChannels.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractInputChannel<T> getInputChannel(int pos) {

		if (pos < this.inputChannels.size()) {
			return this.inputChannels.get(pos);
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkInputChannel<T> createNetworkInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
		final CompressionLevel compressionLevel) {

		final NetworkInputChannel<T> enic = new NetworkInputChannel<T>(inputGate, this.inputChannels.size(),
				this.deserializer, channelID, compressionLevel);
		addInputChannel(enic);

		return enic;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileInputChannel<T> createFileInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
		final CompressionLevel compressionLevel) {

		final FileInputChannel<T> efic = new FileInputChannel<T>(inputGate, this.inputChannels.size(),
				this.deserializer, channelID, compressionLevel);
		addInputChannel(efic);

		return efic;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InMemoryInputChannel<T> createInMemoryInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
		final CompressionLevel compressionLevel) {

		final InMemoryInputChannel<T> eimic = new InMemoryInputChannel<T>(inputGate, this.inputChannels.size(),
				this.deserializer, channelID, compressionLevel);
		addInputChannel(eimic);

		return eimic;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord(final T target) throws IOException, InterruptedException {

		T record = null;

		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		while (true) {

			if (this.channelToReadFrom == -1) {

				if (this.isClosed()) {
					return null;
				}

				this.channelToReadFrom = waitForAnyChannelToBecomeAvailable();
			}
			try {
				if(this.executionstart == -1){
					//save time for arrival of first record
					this.executionstart = System.currentTimeMillis();
				}
				record = this.getInputChannel(this.channelToReadFrom).readRecord(target);
			} catch (EOFException e) {
				// System.out.println("### Caught EOF exception at channel " + channelToReadFrom + "(" +
				// this.getInputChannel(channelToReadFrom).getType().toString() + ")");
				if (this.isClosed()) {
					this.channelToReadFrom = -1;
					return null;
				}
			}

			if (record != null) {
				break;
			} else {
				this.channelToReadFrom = -1;
			}
		}
		this.numrecords++;
		return record;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(int channelIndex) {
		synchronized (this.availableChannels) {

			this.availableChannels.add(Integer.valueOf(channelIndex));
			this.availableChannels.notify();

			if (this.recordAvailabilityListener != null) {
				this.recordAvailabilityListener.reportRecordAvailability(this);
			}
		}
	}

	/**
	 * This method returns the index of a channel which has at least
	 * one record available. The method may block until at least one
	 * channel has become ready.
	 * 
	 * @return the index of the channel which has at least one record available
	 */
	public int waitForAnyChannelToBecomeAvailable() throws InterruptedException {

		synchronized (this.availableChannels) {

			while (this.availableChannels.isEmpty()) {

				// notify the listener objects
				if (this.inputGateListeners != null) {
					for (int i = 0; i < this.inputGateListeners.length; ++i) {
						this.inputGateListeners[i].waitingForAnyChannel();
					}
				}
				this.availableChannels.wait();
			}

			return this.availableChannels.removeFirst().intValue();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		if (this.isClosed) {
			return true;
		}

		for (int i = 0; i < this.getNumberOfInputChannels(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			if (!inputChannel.isClosed()) {
				return false;
			}
		}

		this.isClosed = true;

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException, InterruptedException {

		for (int i = 0; i < this.getNumberOfInputChannels(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			inputChannel.close();
		}

	}

	/**
	 * Returns the list of InputChannels that feed this RecordReader
	 * 
	 * @return the list of InputChannels that feed this RecordReader
	 */
	@Deprecated
	public List<AbstractInputChannel<T>> getInputChannels() {
		return inputChannels;
	}

	/**
	 * Registers a new listener object for this input gate.
	 * 
	 * @param inputGateListener
	 *        the listener object to register
	 */
	public void registerInputGateListener(InputGateListener inputGateListener) {

		if (this.inputGateListeners == null) {
			this.inputGateListeners = new InputGateListener[1];
			this.inputGateListeners[0] = inputGateListener;
		} else {
			InputGateListener[] tmp = this.inputGateListeners;
			this.inputGateListeners = new InputGateListener[tmp.length + 1];
			System.arraycopy(tmp, 0, this.inputGateListeners, 0, tmp.length);
			this.inputGateListeners[tmp.length] = inputGateListener;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Input " + super.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		final Iterator<AbstractInputChannel<T>> it = this.inputChannels.iterator();
		while (it.hasNext()) {
			it.next().transferEvent(event);
		}
	}

	/**
	 * Returns the {@link RecordDeserializer} used by this input gate.
	 * 
	 * @return the {@link RecordDeserializer} used by this input gate
	 */
	public RecordDeserializer<T> getRecordDeserializer() {

		return this.deserializer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractInputChannel<T>> it = this.inputChannels.iterator();
		while (it.hasNext()) {
			it.next().releaseAllResources();
		}
	}

	@Override
	public void activateInputChannels() throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerRecordAvailabilityListener(final RecordAvailabilityListener<T> listener) {

		synchronized (this.availableChannels) {

			if (this.recordAvailabilityListener != null) {
				throw new IllegalStateException(this.recordAvailabilityListener
					+ " is already registered as a record availability listener");
			}

			this.recordAvailabilityListener = listener;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasRecordAvailable() throws IOException, InterruptedException {

		if (this.channelToReadFrom == -1) {

			if (this.isClosed()) {
				return true;
			}

			synchronized (this.availableChannels) {

				return !(this.availableChannels.isEmpty());
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getExecutionStart(){
		return this.executionstart;
	}
	public void notifyDataUnitConsumed(final int channelIndex) {

		this.channelToReadFrom = -1;

	}

	@Override
	public int getNumRecords() {
		return this.numrecords;
	}
}
