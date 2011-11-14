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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.event.task.EventNotificationManager;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.FileInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.io.channels.direct.InMemoryInputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.ClassUtils;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * In Nephele input gates are a specialization of general gates and connect input channels and record readers. As
 * channels, input
 * gates are always parameterized to a specific type of record which they can transport. In contrast to output gates
 * input gates
 * can be associated with a {@link DistributionPattern} object which dictates the concrete wiring between two groups of
 * vertices.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public class InputGate<T extends Record> extends Gate<T> implements IOReadableWritable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(InputGate.class);

	/**
	 * The list of input channels attached to this input gate.
	 */
	private final ArrayList<AbstractInputChannel<T>> inputChannels = new ArrayList<AbstractInputChannel<T>>();

	/**
	 * The distribution pattern to determine how to wire the channels.
	 */
	private DistributionPattern distributionPattern = null;

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

	private final EventNotificationManager eventNotificationManager = new EventNotificationManager();

	private int channelToReadFrom = -1;

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	/**
	 * Constructs a new input gate.
	 * 
	 * @param inputClass
	 *        the class of the record that can be transported through this gate
	 * @param index
	 *        the index assigned to this input gate at the {@link Environment} object
	 * @param distributionPattern
	 *        the distribution pattern to determine the concrete wiring between to groups of vertices
	 */
	public InputGate(RecordDeserializer<T> deserializer, int index, DistributionPattern distributionPattern) {
		this.index = index;
		setDeserializer(deserializer);
		if (distributionPattern == null) {
			this.distributionPattern = new BipartiteDistributionPattern();
		} else {
			this.distributionPattern = distributionPattern;
		}
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
	 * Removes all input channels from the input gate.
	 */
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
	 * Returns the {@link DistributionPattern} associated with this input gate.
	 * 
	 * @return the {@link DistributionPattern} associated with this input gate
	 */
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
	 * Returns the number of input channels associated with this input gate.
	 * 
	 * @return the number of input channels associated with this input gate
	 */
	public int getNumberOfInputChannels() {

		return this.inputChannels.size();
	}

	/**
	 * Returns the input channel from position <code>pos</code> of the gate's internal channel list.
	 * 
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such position does not exist.
	 */
	public AbstractInputChannel<T> getInputChannel(int pos) {

		if (pos < this.inputChannels.size()) {
			return this.inputChannels.get(pos);
		}

		return null;
	}

	/**
	 * Creates a new network input channel and assigns it to the input gate.
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new network input channel
	 */
	public NetworkInputChannel<T> createNetworkInputChannel(ChannelID channelID, CompressionLevel compressionLevel) {

		final NetworkInputChannel<T> enic = new NetworkInputChannel<T>(this, this.inputChannels.size(), deserializer,
			channelID, compressionLevel);
		addInputChannel(enic);

		return enic;
	}

	/**
	 * Creates a new file input channel and assigns it to the input gate.
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new file input channel
	 */
	public FileInputChannel<T> createFileInputChannel(ChannelID channelID, CompressionLevel compressionLevel) {

		final FileInputChannel<T> efic = new FileInputChannel<T>(this, this.inputChannels.size(), deserializer,
			channelID, compressionLevel);
		addInputChannel(efic);

		return efic;
	}

	/**
	 * Creates a new in-memory input channel and assigns it to the input gate.
	 * 
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @return the new in-memory input channel
	 */
	public InMemoryInputChannel<T> createInMemoryInputChannel(ChannelID channelID, CompressionLevel compressionLevel) {

		final InMemoryInputChannel<T> eimic = new InMemoryInputChannel<T>(this, this.inputChannels.size(),
			deserializer, channelID, compressionLevel);
		addInputChannel(eimic);

		return eimic;
	}

	/**
	 * Reads a record from one of the associated input channels. The channels are
	 * chosen in a way so that channels with available records are preferred.
	 * The operation may block until at least one of the associated input channel
	 * is able to provide a record.
	 * 
	 * @return the record read from one of the input channels or <code>null</code> if all channels are already closed.
	 * @throws ExecutionFailureException
	 *         thrown if an error occurred while reading the channels
	 */

	public T readRecord() throws IOException, InterruptedException {

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
				record = this.getInputChannel(this.channelToReadFrom).readRecord();
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

		return record;
	}

	/**
	 * Notify the gate that the channel with the given index has
	 * at least one record available.
	 */
	public void notifyRecordIsAvailable(int channelIndex) {
		LOG.info("channelIndex " + channelIndex);

		int org = channelIndex;
		if(channelIndex < 0)
			channelIndex += 100;
		synchronized (this.availableChannels) {

			this.availableChannels.add(Integer.valueOf(channelIndex));
			LOG.info("notify " + org);
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

	// TODO: See if type safety can be improved here
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {

		super.read(in);

		final int numInputChannels = in.readInt();

		for (int i = 0; i < numInputChannels; i++) {

			final ChannelID channelID = new ChannelID();
			channelID.read(in);
			final CompressionLevel compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);

			final String className = StringRecord.readString(in);
			Class<? extends IOReadableWritable> c = null;
			try {
				c = ClassUtils.getRecordByName(className);
			} catch (ClassNotFoundException e) {
				LOG.error(e);
			}

			if (c == null) {
				throw new IOException("Class is null!");
			}

			AbstractInputChannel<T> eic = null;
			try {
				final Constructor<AbstractInputChannel<T>> constructor = (Constructor<AbstractInputChannel<T>>) c
						.getDeclaredConstructor(this.getClass(), int.class, RecordDeserializer.class, ChannelID.class,
							CompressionLevel.class);
				if (constructor == null) {
					throw new IOException("Constructor is null!");
				}
				constructor.setAccessible(true);
				eic = constructor.newInstance(this, i, deserializer, channelID, compressionLevel);
			} catch (SecurityException e) {
				LOG.error(e);
			} catch (NoSuchMethodException e) {
				LOG.error(e);
			} catch (IllegalArgumentException e) {
				LOG.error(e);
			} catch (InstantiationException e) {
				LOG.error(e);
			} catch (IllegalAccessException e) {
				LOG.error(e);
			} catch (InvocationTargetException e) {
				LOG.error(e);
			}
			if (eic == null) {
				throw new IOException("Created input channel is null!");
			}

			eic.read(in);
			addInputChannel(eic);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);

		// Connected input channels
		out.writeInt(this.getNumberOfInputChannels());
		for (int i = 0; i < getNumberOfInputChannels(); i++) {
			getInputChannel(i).getID().write(out);
			EnumUtils.writeEnum(out, getInputChannel(i).getCompressionLevel());
			StringRecord.writeString(out, getInputChannel(i).getClass().getName());
			getInputChannel(i).write(out);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException {

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
	 * Immediately closes the input gate and all its input channels. The corresponding
	 * output channels are notified. Any remaining records in any buffers or queue is considered
	 * irrelevant and is discarded.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the channels to be closed
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the channels
	 */
	public void close() throws InterruptedException, IOException {

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
	 * Subscribes the listener object to receive events of the given type.
	 * 
	 * @param eventListener
	 *        the listener object to register
	 * @param eventType
	 *        the type of event to register the listener for
	 */
	public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * Removes the subscription for events of the given type for the listener object.
	 * 
	 * @param eventListener
	 *        the listener object to cancel the subscription for
	 * @param eventType
	 *        the type of the event to cancel the subscription for
	 */
	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * Publishes an event.
	 * 
	 * @param event
	 *        the event to be published
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the event
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the event to be published
	 */
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		final Iterator<AbstractInputChannel<T>> it = this.inputChannels.iterator();
		while (it.hasNext()) {
			it.next().transferEvent(event);
		}
	}

	/**
	 * Passes a received event on to the event notification manager so it cam ne dispatched.
	 * 
	 * @param event
	 *        the event to pass on to the notification manager
	 */
	public void deliverEvent(AbstractTaskEvent event) {

		this.eventNotificationManager.deliverEvent((AbstractTaskEvent) event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractInputChannel<T>> it = this.inputChannels.iterator();
		while (it.hasNext()) {
			it.next().releaseResources();
		}
	}

	boolean hasRecordAvailable() throws IOException {

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

	void registerRecordAvailabilityListener(final RecordAvailabilityListener<T> listener) {

		synchronized (this.availableChannels) {

			if (this.recordAvailabilityListener != null) {
				throw new IllegalStateException(this.recordAvailabilityListener
					+ " is already registered as a record availability listener");
			}

			this.recordAvailabilityListener = listener;
		}
	}
}
