/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.channels;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.gates.InputChannelResult;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.Envelope;
import eu.stratosphere.runtime.io.gates.InputGate;
import eu.stratosphere.runtime.io.serialization.AdaptiveSpanningRecordDeserializer;
import eu.stratosphere.runtime.io.serialization.RecordDeserializer;
import eu.stratosphere.runtime.io.serialization.RecordDeserializer.DeserializationResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 * InputChannel is an abstract base class to all different kinds of concrete
 * input channels that can be used. Input channels are always parameterized to
 * a specific type that can be transported through the channel.

 * @param <T> The Type of the record that can be transported through the channel.
 */
public class InputChannel<T extends IOReadableWritable> extends Channel implements BufferProvider {

	private final InputGate<T> inputGate;

	/**
	 * The log object used to report warnings and errors.
	 */
	private static final Log LOG = LogFactory.getLog(InputChannel.class);

	/**
	 * The deserializer used to deserialize records.
	 */
	private final RecordDeserializer<T> deserializer;

	/**
	 * Buffer for the uncompressed (raw) data.
	 */
	private Buffer dataBuffer;

	private AbstractTaskEvent currentEvent;

	/**
	 * The exception observed in this channel while processing the buffers. Checked and thrown
	 * per-buffer.
	 */
	private volatile IOException ioException;

	/**
	 * Stores the number of bytes read through this input channel since its instantiation.
	 */
	private long amountOfDataTransmitted;

	private volatile boolean brokerAggreedToCloseChannel;

	// -------------------------------------------------------------------------------------------

	private int lastReceivedEnvelope = -1;

	private ChannelID lastSourceID = null;

	private boolean destroyCalled = false;

	// ----------------------

	private Queue<Envelope> queuedEnvelopes = new ArrayDeque<Envelope>();

	private Iterator<AbstractEvent> pendingEvents;

	/**
	 * Constructs an input channel with a given input gate associated.
	 * 
	 * @param inputGate
	 *        the input gate this channel is connected to
	 * @param channelIndex
	 *        the index of the channel in the input gate
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 */
	public InputChannel(final InputGate<T> inputGate, final int channelIndex, final ChannelID channelID,
						final ChannelID connectedChannelID, ChannelType type) {
		super(channelIndex, channelID, connectedChannelID, type);
		this.inputGate = inputGate;
		this.deserializer = new AdaptiveSpanningRecordDeserializer<T>();
	}

	/**
	 * Returns the input gate associated with the input channel.
	 * 
	 * @return the input gate associated with the input channel.
	 */
	public InputGate<T> getInputGate() {
		return this.inputGate;
	}

	/**
	 * Reads a record from the input channel. If currently no record is available the method
	 * returns <code>null</code>. If the channel is closed (i.e. no more records will be received), the method
	 * throws an {@link EOFException}.
	 * 
	 * @return a record that has been transported through the channel or <code>null</code> if currently no record is
	 *         available
	 * @throws IOException
	 *         thrown if the input channel is already closed {@link EOFException} or a transmission error has occurred
	 */
//	public abstract InputChannelResult readRecord(T target) throws IOException;

	/**
	 * Immediately closes the input channel. The corresponding output channels are
	 * notified if necessary. Any remaining records in any buffers or queue is considered
	 * irrelevant and is discarded.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the channel to close
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the channel
	 */
//	public abstract void close() throws IOException, InterruptedException;



	@Override
	public boolean isInputChannel() {
		return true;
	}


	@Override
	public JobID getJobID() {
		return this.inputGate.getJobID();
	}

//	public abstract AbstractTaskEvent getCurrentEvent();

	private DeserializationResult lastDeserializationResult;


	public InputChannelResult readRecord(T target) throws IOException {
		if (this.dataBuffer == null) {
			if (isClosed()) {
				return InputChannelResult.END_OF_STREAM;
			}

			// get the next element we need to handle (buffer or event)
			BufferOrEvent boe = getNextBufferOrEvent();

			if (boe == null) {
				throw new IllegalStateException("Input channel was queries for data even though none was announced available.");
			}

			// handle events
			if (boe.isEvent())
			{
				// sanity check: an event may only come after a complete record.
				if (this.deserializer.hasUnfinishedData()) {
					throw new IllegalStateException("Channel received an event before completing the current partial record.");
				}

				AbstractEvent evt = boe.getEvent();
				if (evt.getClass() == ChannelCloseEvent.class) {
					this.brokerAggreedToCloseChannel = true;
					return InputChannelResult.END_OF_STREAM;
				}
				else if (evt.getClass() == EndOfSuperstepEvent.class) {
					return InputChannelResult.END_OF_SUPERSTEP;
				}
				else if (evt instanceof AbstractTaskEvent) {
					this.currentEvent = (AbstractTaskEvent) evt;
					return InputChannelResult.TASK_EVENT;
				}
				else {
					LOG.error("Received unknown event: " + evt);
					return InputChannelResult.NONE;
				}
			} else {
				// buffer case
				this.dataBuffer = boe.getBuffer();
				this.deserializer.setNextMemorySegment(this.dataBuffer.getMemorySegment(), this.dataBuffer.size());
			}
		}

		DeserializationResult deserializationResult = this.deserializer.getNextRecord(target);
		this.lastDeserializationResult = deserializationResult;

		if (deserializationResult.isBufferConsumed()) {
			releasedConsumedReadBuffer(this.dataBuffer);
			this.dataBuffer = null;
		}

		if (deserializationResult == DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER) {
			return InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;
		} else if (deserializationResult == DeserializationResult.LAST_RECORD_FROM_BUFFER) {
			return InputChannelResult.LAST_RECORD_FROM_BUFFER;
		} else if (deserializationResult == DeserializationResult.PARTIAL_RECORD) {
			return InputChannelResult.NONE;
		} else {
			throw new IllegalStateException();
		}
	}

	@Override
	public ChannelType getChannelType() {
		return null;
	}

	@Override
	public boolean isClosed() throws IOException{
		if (this.ioException != null) {
			throw new IOException("An error occurred in the channel: " + this.ioException.getMessage(), this.ioException);
		} else {
			return this.brokerAggreedToCloseChannel;
		}
	}

	public void close() throws IOException, InterruptedException {

		this.deserializer.clear();
		if (this.dataBuffer != null) {
			releasedConsumedReadBuffer(this.dataBuffer);
			this.dataBuffer = null;
		}

		// This code fragment makes sure the isClosed method works in case the channel input has not been fully consumed
		while (!this.brokerAggreedToCloseChannel)
		{
			BufferOrEvent next = getNextBufferOrEvent();
			if (next != null) {
				if (next.isEvent()) {
					if (next.getEvent() instanceof ChannelCloseEvent) {
						this.brokerAggreedToCloseChannel = true;
					}
				} else {
					releasedConsumedReadBuffer(next.getBuffer());
				}
			} else {
				Thread.sleep(200);
			}
		}

		// Send close event to indicate the input channel has successfully
		// processed all data it is interested in.
		transferEventToOutputChannel(new ChannelCloseEvent());
	}


	private void releasedConsumedReadBuffer(Buffer buffer) {
		this.amountOfDataTransmitted += buffer.size();
		buffer.recycleBuffer();
	}


	public void notifyGateThatInputIsAvailable() {
		this.getInputGate().notifyRecordIsAvailable(getIndex());
	}


	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {
		transferEventToOutputChannel(event);
	}


	public void reportIOException(IOException ioe) {
		this.ioException = ioe;
	}


	@Override
	public void releaseAllResources() {
		this.brokerAggreedToCloseChannel = true;
		this.deserializer.clear();

		// The buffers are recycled by the input channel wrapper
	}

	/**
	 * Notify the channel that a data unit has been consumed.
	 */
	public void notifyDataUnitConsumed() {
		this.getInputGate().notifyDataUnitConsumed(getIndex());
	}

	public AbstractTaskEvent getCurrentEvent() {
		AbstractTaskEvent e = this.currentEvent;
		this.currentEvent = null;
		return e;
	}

	// InputChannelContext

	@Override
	public void queueEnvelope(Envelope envelope) {
		// The sequence number of the envelope to be queued
		final int sequenceNumber = envelope.getSequenceNumber();

		synchronized (this.queuedEnvelopes) {

			if (this.destroyCalled) {
				final Buffer buffer = envelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}
				return;
			}

			final int expectedSequenceNumber = this.lastReceivedEnvelope + 1;
			if (sequenceNumber != expectedSequenceNumber) {
				// This is a problem, now we are actually missing some data
				reportIOException(new IOException("Expected data packet " + expectedSequenceNumber + " but received " + sequenceNumber));

				// notify that something (an exception) is available
				notifyGateThatInputIsAvailable();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Input channel " + this.toString() + " expected envelope " + expectedSequenceNumber
							+ " but received " + sequenceNumber);
				}

				// rescue the buffer
				final Buffer buffer = envelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}
			} else {

				this.queuedEnvelopes.add(envelope);
				this.lastReceivedEnvelope = sequenceNumber;
				this.lastSourceID = envelope.getSource();

				// Notify the channel about the new data. notify as much as there is (buffer plus once per event)
				if (envelope.getBuffer() != null) {
					notifyGateThatInputIsAvailable();
				}

				List<? extends AbstractEvent> events = envelope.deserializeEvents();

				if (events != null) {
					for (int i = 0; i < events.size(); i++) {
						notifyGateThatInputIsAvailable();
					}
				}
			}
		}
	}

	@Override
	public void destroy() {
		final Queue<Buffer> buffersToRecycle = new ArrayDeque<Buffer>();

		synchronized (this.queuedEnvelopes) {
			this.destroyCalled = true;

			while (!this.queuedEnvelopes.isEmpty()) {
				final Envelope envelope = this.queuedEnvelopes.poll();
				if (envelope.getBuffer() != null) {
					buffersToRecycle.add(envelope.getBuffer());
				}
			}
		}

		while (!buffersToRecycle.isEmpty()) {
			buffersToRecycle.poll().recycleBuffer();
		}
	}

	public void logQueuedEnvelopes() {
		int numberOfQueuedEnvelopes = 0;
		int numberOfQueuedEnvelopesWithMemoryBuffers = 0;
		int numberOfQueuedEnvelopesWithFileBuffers = 0;

		synchronized (this.queuedEnvelopes) {

			final Iterator<Envelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {

				final Envelope envelope = it.next();
				++numberOfQueuedEnvelopes;
				final Buffer buffer = envelope.getBuffer();
				if (buffer == null) {
					continue;
				}

				++numberOfQueuedEnvelopesWithMemoryBuffers;
			}
		}

		System.out.println("\t\t" + this.toString() + ": " + numberOfQueuedEnvelopes + " ("
				+ numberOfQueuedEnvelopesWithMemoryBuffers + ", " + numberOfQueuedEnvelopesWithFileBuffers + ")");

	}

	@Override
	public Buffer requestBuffer(int minBufferSize) throws IOException {
		return this.inputGate.requestBuffer(minBufferSize);
	}

	@Override
	public Buffer requestBufferBlocking(int minBufferSize) throws IOException, InterruptedException {
		return this.inputGate.requestBufferBlocking(minBufferSize);
	}

	@Override
	public int getBufferSize() {
		return this.inputGate.getBufferSize();
	}

	@Override
	public void reportAsynchronousEvent() {
		this.inputGate.reportAsynchronousEvent();
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener listener) {
		return this.inputGate.registerBufferAvailabilityListener(listener);
	}

	// ChannelBroker

	public BufferOrEvent getNextBufferOrEvent() throws IOException {
		// return pending events first
		if (this.pendingEvents != null) {
			// if the field is not null, it must always have a next value!
			BufferOrEvent next = new BufferOrEvent(this.pendingEvents.next());
			if (!this.pendingEvents.hasNext()) {
				this.pendingEvents = null;
			}
			return next;
		}

		// if no events are pending, get the next buffer
		Envelope nextEnvelope;
		synchronized (this.queuedEnvelopes) {
			if (this.queuedEnvelopes.isEmpty()) {
				return null;
			}
			nextEnvelope = this.queuedEnvelopes.poll();
		}

		// schedule events as pending, because events come always after the buffer!
		List<AbstractEvent> events = (List<AbstractEvent>) nextEnvelope.deserializeEvents();
		Iterator<AbstractEvent> eventsIt = events.iterator();
		if (eventsIt.hasNext()) {
			this.pendingEvents = eventsIt;
		}

		// get the buffer, if there is one
		if (nextEnvelope.getBuffer() != null) {
			return new BufferOrEvent(nextEnvelope.getBuffer());
		}
		else if (this.pendingEvents != null) {
			// if the field is not null, it must always have a next value!
			BufferOrEvent next = new BufferOrEvent(this.pendingEvents.next());
			if (!this.pendingEvents.hasNext()) {
				this.pendingEvents = null;
			}

			return next;
		}
		else {
			// no buffer and no events, this should be an error
			throw new IOException("Received an envelope with neither data nor events.");
		}
	}

	public void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException {
		Envelope ephemeralEnvelope = new Envelope(0, getJobID(), getID());
		ephemeralEnvelope.serializeEventList(Arrays.asList(event));

		this.envelopeDispatcher.dispatchFromInputChannel(ephemeralEnvelope);
	}
}
