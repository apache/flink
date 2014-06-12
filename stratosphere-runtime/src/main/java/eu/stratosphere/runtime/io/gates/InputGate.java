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

package eu.stratosphere.runtime.io.gates;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import eu.stratosphere.nephele.deployment.ChannelDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.GateDeploymentDescriptor;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.GlobalBufferPool;
import eu.stratosphere.runtime.io.network.bufferprovider.LocalBufferPool;
import eu.stratosphere.runtime.io.network.bufferprovider.LocalBufferPoolOwner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.runtime.io.channels.InputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * In Nephele input gates are a specialization of general gates and connect input channels and record readers. As
 * channels, input gates are always parameterized to a specific type of record which they can transport. In contrast to
 * output gates input gates can be associated with a {@link eu.stratosphere.runtime.io.serialization.io.DistributionPattern} object which dictates the concrete
 * wiring between two groups of vertices.
 * 
 * @param <T> The type of record that can be transported through this gate.
 */
public class InputGate<T extends IOReadableWritable> extends Gate<T> implements BufferProvider, LocalBufferPoolOwner {
	
	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(InputGate.class);

	/**
	 * The array of input channels attached to this input gate.
	 */
	private InputChannel<T>[] channels;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final BlockingQueue<Integer> availableChannels = new LinkedBlockingQueue<Integer>();

	/**
	 * The listener object to be notified when a channel has at least one record available.
	 */
	private final AtomicReference<RecordAvailabilityListener<T>> recordAvailabilityListener = new AtomicReference<RecordAvailabilityListener<T>>(null);
	
	
	private AbstractTaskEvent currentEvent;

	/**
	 * If the value of this variable is set to <code>true</code>, the input gate is closed.
	 */
	private boolean isClosed = false;

	/**
	 * The channel to read from next.
	 */
	private int channelToReadFrom = -1;

	private LocalBufferPool bufferPool;

	/**
	 * Constructs a new runtime input gate.
	 * 
	 * @param jobID
	 *        the ID of the job this input gate belongs to
	 * @param gateID
	 *        the ID of the gate
	 * @param index
	 *        the index assigned to this input gate at the {@link Environment} object
	 */
	public InputGate(final JobID jobID, final GateID gateID, final int index) {
		super(jobID, gateID, index);
	}

	public void initializeChannels(GateDeploymentDescriptor inputGateDescriptor){
		channels = new InputChannel[inputGateDescriptor.getNumberOfChannelDescriptors()];

		setChannelType(inputGateDescriptor.getChannelType());

		final int nicdd = inputGateDescriptor.getNumberOfChannelDescriptors();

		for(int i = 0; i < nicdd; i++){
			final ChannelDeploymentDescriptor cdd = inputGateDescriptor.getChannelDescriptor(i);
			channels[i] = new InputChannel<T>(this, i, cdd.getInputChannelID(),
					cdd.getOutputChannelID(), getChannelType());
		}
	}

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
		return this.channels.length;
	}

	/**
	 * Returns the input channel from position <code>pos</code> of the gate's internal channel list.
	 *
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such position does not exist.
	 */
	public InputChannel<T> getInputChannel(int pos) {
		return this.channels[pos];
	}

	public InputChannel<T>[] channels() {
		return this.channels;
	}

	/**
	 * Reads a record from one of the associated input channels. Channels are read such that one buffer from a channel is
	 * consecutively consumed. The buffers in turn are consumed in the order in which they arrive.
	 * Note that this method is not guaranteed to return a record, because the currently available channel data may not always
	 * constitute an entire record, when events or partial records are part of the data.
	 *
	 * When called even though no data is available, this call will block until data is available, so this method should be called
	 * when waiting is desired (such as when synchronously consuming a single gate) or only when it is known that data is available
	 * (such as when reading a union of multiple input gates).
	 *
	 * @param target The record object into which to construct the complete record.
	 * @return The result indicating whether a complete record is available, a event is available, only incomplete data
	 *         is available (NONE), or the gate is exhausted.
	 * @throws IOException Thrown when an error occurred in the network stack relating to this channel.
	 * @throws InterruptedException Thrown, when the thread working on this channel is interrupted.
	 */
	public InputChannelResult readRecord(T target) throws IOException, InterruptedException {

		if (this.channelToReadFrom == -1) {
			if (this.isClosed()) {
				return InputChannelResult.END_OF_STREAM;
			}
				
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
				
			this.channelToReadFrom = waitForAnyChannelToBecomeAvailable();
		}
			
		InputChannelResult result = this.getInputChannel(this.channelToReadFrom).readRecord(target);
		switch (result) {
			case INTERMEDIATE_RECORD_FROM_BUFFER: // full record and we can stay on the same channel
				return InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;
				
			case LAST_RECORD_FROM_BUFFER: // full record, but we must switch the channel afterwards
				this.channelToReadFrom = -1;
				return InputChannelResult.LAST_RECORD_FROM_BUFFER;
				
			case END_OF_SUPERSTEP:
				this.channelToReadFrom = -1;
				return InputChannelResult.END_OF_SUPERSTEP;
				
			case TASK_EVENT: // task event
				this.currentEvent = this.getInputChannel(this.channelToReadFrom).getCurrentEvent();
				this.channelToReadFrom = -1;	// event always marks a unit as consumed
				return InputChannelResult.TASK_EVENT;
					
			case NONE: // internal event or an incomplete record that needs further chunks
				// the current unit is exhausted
				this.channelToReadFrom = -1;
				return InputChannelResult.NONE;
				
			case END_OF_STREAM: // channel is done
				this.channelToReadFrom = -1;
				return isClosed() ? InputChannelResult.END_OF_STREAM : InputChannelResult.NONE;
				
			default:   // silence the compiler
				throw new RuntimeException();
		}
	}

	public AbstractTaskEvent getCurrentEvent() {
		AbstractTaskEvent e = this.currentEvent;
		this.currentEvent = null;
		return e;
	}

	/**
	 * Notify the gate that the channel with the given index has
	 * at least one record available.
	 *
	 * @param channelIndex
	 *        the index of the channel which has at least one record available
	 */
	public void notifyRecordIsAvailable(int channelIndex) {
		this.availableChannels.add(Integer.valueOf(channelIndex));

		RecordAvailabilityListener<T> listener = this.recordAvailabilityListener.get();
		if (listener != null) {
			listener.reportRecordAvailability(this);
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
		return this.availableChannels.take().intValue();
	}


	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		if (this.isClosed) {
			return true;
		}

		for (int i = 0; i < this.getNumberOfInputChannels(); i++) {
			final InputChannel<T> inputChannel = this.channels[i];
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
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the gate
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the gate to be closed
	 */
	public void close() throws IOException, InterruptedException {

		for (int i = 0; i < this.getNumberOfInputChannels(); i++) {
			final InputChannel<T> inputChannel = this.channels[i];
			inputChannel.close();
		}

	}


	@Override
	public String toString() {
		return "Input " + super.toString();
	}


	@Override
	public void publishEvent(AbstractEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		for(int i=0; i< getNumberOfChannels(); i++){
			channels[i].transferEvent(event);
		}
	}


	@Override
	public void releaseAllChannelResources() {

		for(int i=0; i< getNumberOfChannels(); i++){
			channels[i].releaseAllResources();
		}
	}

	/**
	 * Registers a {@link RecordAvailabilityListener} with this input gate.
	 *
	 * @param listener
	 *        the listener object to be registered
	 */
	public void registerRecordAvailabilityListener(final RecordAvailabilityListener<T> listener) {
		if (!this.recordAvailabilityListener.compareAndSet(null, listener)) {
			throw new IllegalStateException(this.recordAvailabilityListener
				+ " is already registered as a record availability listener");
		}
	}

	/**
	 * Notify the gate that is has consumed a data unit from the channel with the given index
	 *
	 * @param channelIndex
	 *        the index of the channel from which a data unit has been consumed
	 */
	public void notifyDataUnitConsumed(int channelIndex) {
		this.channelToReadFrom = -1;
	}

	//

	@Override
	public Buffer requestBuffer(int minBufferSize) throws IOException {
		return this.bufferPool.requestBuffer(minBufferSize);
	}

	@Override
	public Buffer requestBufferBlocking(int minBufferSize) throws IOException, InterruptedException {
		return this.bufferPool.requestBufferBlocking(minBufferSize);
	}

	@Override
	public int getBufferSize() {
		return this.bufferPool.getBufferSize();
	}

	@Override
	public int getNumberOfChannels() {
		return getNumberOfInputChannels();
	}

	@Override
	public void setDesignatedNumberOfBuffers(int numBuffers) {
		this.bufferPool.setNumDesignatedBuffers(numBuffers);
	}

	@Override
	public void clearLocalBufferPool() {
		this.bufferPool.destroy();
	}

	@Override
	public void registerGlobalBufferPool(GlobalBufferPool globalBufferPool) {
		this.bufferPool = new LocalBufferPool(globalBufferPool, 1);
	}

	@Override
	public void logBufferUtilization() {
		LOG.info(String.format("\t%s: %d available, %d requested, %d designated",
				this,
				this.bufferPool.numAvailableBuffers(),
				this.bufferPool.numRequestedBuffers(),
				this.bufferPool.numDesignatedBuffers()));
	}

	@Override
	public void reportAsynchronousEvent() {
		this.bufferPool.reportAsynchronousEvent();
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener listener) {
		return this.bufferPool.registerBufferAvailabilityListener(listener);
	}
}
