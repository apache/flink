/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;
import java.util.ArrayDeque;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The barrier buffer is {@link CheckpointBarrierHandler} that blocks inputs with barriers until
 * all inputs have received the barrier for a given checkpoint.
 * 
 * <p>To avoid back-pressuring the input streams (which may cause distributed deadlocks), the
 * BarrierBuffer continues receiving buffers from the blocked channels and stores them internally until 
 * the blocks are released.</p>
 */
public class BarrierBuffer implements CheckpointBarrierHandler, Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);
	
	/** The gate that the buffer draws its input from */
	private final InputGate inputGate;

	/** Flags that indicate whether a channel is currently blocked/buffered */
	private final boolean[] blockedChannels;
	
	/** The total number of channels that this buffer handles data from */
	private final int totalNumberOfInputChannels;
	
	/** To utility to write blocked data to a file channel */
	private final BufferSpiller bufferSpiller;

	/** The pending blocked buffer/event sequences. Must be consumed before requesting
	 * further data from the input gate. */
	private final ArrayDeque<BufferSpiller.SpilledBufferOrEventSequence> queuedBuffered;

	/** The sequence of buffers/events that has been unblocked and must now be consumed
	 * before requesting further data from the input gate */
	private BufferSpiller.SpilledBufferOrEventSequence currentBuffered;

	/** Handler that receives the checkpoint notifications */
	private EventListener<CheckpointBarrier> checkpointHandler;

	/** The ID of the checkpoint for which we expect barriers */
	private long currentCheckpointId = -1L;

	/** The number of received barriers (= number of blocked/buffered channels) */
	private int numBarriersReceived;
	
	/** The number of already closed channels */
	private int numClosedChannels;
	
	/** Flag to indicate whether we have drawn all available input */
	private boolean endOfStream;


	private int returnedBuffers;
	
	private int spilledBuffers;
	
	private int reReadBuffers;
	
	
	private Thread debugPrinter;
	
	private volatile boolean printerRunning = true;
	
	/**
	 * 
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param ioManager The I/O manager that gives access to the temp directories.
	 * 
	 * @throws IOException Thrown, when the spilling to temp files cannot be initialized.
	 */
	public BarrierBuffer(InputGate inputGate, IOManager ioManager) throws IOException {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.blockedChannels = new boolean[this.totalNumberOfInputChannels];
		
		this.bufferSpiller = new BufferSpiller(ioManager, inputGate.getPageSize());
		this.queuedBuffered = new ArrayDeque<BufferSpiller.SpilledBufferOrEventSequence>();
		
		this.debugPrinter = new Thread(this, "BB debugger");
		this.debugPrinter.setDaemon(true);
		this.debugPrinter.start();
	}

	// ------------------------------------------------------------------------
	//  Buffer and barrier handling
	// ------------------------------------------------------------------------

	@Override
	public BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			BufferOrEvent next;
			if (currentBuffered == null) {
				next = inputGate.getNextBufferOrEvent();
			}
			else {
				next = currentBuffered.getNext();
				if (next == null) {
					completeBufferedSequence();
					return getNextNonBlocked();
				}
				else if (next.isBuffer()) {
					reReadBuffers++;
				}
			}
			
			if (next != null) {
				if (isBlocked(next.getChannelIndex())) {
					// if the channel is blocked we, we just store the BufferOrEvent
					bufferSpiller.add(next);
					if (next.isBuffer()) {
						spilledBuffers++;
					}
				}
				else if (next.isBuffer()) {
					returnedBuffers++;
					return next;
				}
				else if (next.getEvent().getClass() == CheckpointBarrier.class) {
					if (!endOfStream) {
						// process barriers only if there is a chance of the checkpoint completing
						processBarrier((CheckpointBarrier) next.getEvent(), next.getChannelIndex());
					}
				}
				else {
					if (next.getEvent().getClass() == EndOfPartitionEvent.class) {
						numClosedChannels++;
						// no chance to complete this checkpoint
						releaseBlocks();
					}
					return next;
				}
			}
			else if (!endOfStream) {
				// end of stream. we feed the data that is still buffered
				endOfStream = true;
				releaseBlocks();
				return getNextNonBlocked();
			}
			else {
				return null;
			}
		}
	}
	
	private void completeBufferedSequence() throws IOException {
		currentBuffered.cleanup();
		currentBuffered = queuedBuffered.pollFirst();
		if (currentBuffered != null) {
			currentBuffered.open();
		}
	}
	
	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws IOException {
		final long barrierId = receivedBarrier.getId();

		if (numBarriersReceived > 0) {
			// subsequent barrier of a checkpoint.
			if (barrierId == currentCheckpointId) {
				// regular case
				onBarrier(channelIndex);
			}
			else if (barrierId > currentCheckpointId) {
				// we did not complete the current checkpoint
				LOG.warn("Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.", barrierId, currentCheckpointId);

				releaseBlocks();
				currentCheckpointId = barrierId;
				onBarrier(channelIndex);
			}
			else {
				// ignore trailing barrier from aborted checkpoint
				return;
			}
			
		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			currentCheckpointId = barrierId;
			onBarrier(channelIndex);
		}
		else {
			// trailing barrier from previous (skipped) checkpoint
			return;
		}

		// check if we have all barriers
		if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received all barrier, triggering checkpoint {} at {}",
						receivedBarrier.getId(), receivedBarrier.getTimestamp());
			}

			if (checkpointHandler != null) {
				checkpointHandler.onEvent(receivedBarrier);
			}
			
			releaseBlocks();
		}
	}
	
	@Override
	public void registerCheckpointEventHandler(EventListener<CheckpointBarrier> checkpointHandler) {
		if (this.checkpointHandler == null) {
			this.checkpointHandler = checkpointHandler;
		}
		else {
			throw new IllegalStateException("BarrierBuffer already has a registered checkpoint handler");
		}
	}
	
	@Override
	public boolean isEmpty() {
		return currentBuffered == null;
	}

	@Override
	public void cleanup() throws IOException {
		printerRunning = false;
		debugPrinter.interrupt();
		
		bufferSpiller.close();
		if (currentBuffered != null) {
			currentBuffered.cleanup();
		}
		for (BufferSpiller.SpilledBufferOrEventSequence seq : queuedBuffered) {
			seq.cleanup();
		}
	}
	
	/**
	 * Checks whether the channel with the given index is blocked.
	 * 
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	private boolean isBlocked(int channelIndex) {
		return blockedChannels[channelIndex];
	}
	
	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 * 
	 * @param channelIndex The channel index to block.
	 */
	private void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			blockedChannels[channelIndex] = true;
			numBarriersReceived++;
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received barrier from channel " + channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint and input stream");
		}
	}

	/**
	 * Releases the blocks on all channels. Makes sure the just written data
	 * is the next to be consumed.
	 */
	private void releaseBlocks() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Releasing blocks");
		}

		for (int i = 0; i < blockedChannels.length; i++) {
			blockedChannels[i] = false;
		}
		numBarriersReceived = 0;

		if (currentBuffered == null) {
			// common case: no more buffered data
			currentBuffered = bufferSpiller.rollOver();
			if (currentBuffered != null) {
				currentBuffered.open();
			}
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			
			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			BufferSpiller.SpilledBufferOrEventSequence bufferedNow = bufferSpiller.rollOverWithNewBuffer();
			if (bufferedNow != null) {
				bufferedNow.open();
				queuedBuffered.addFirst(currentBuffered);
				currentBuffered = bufferedNow;
			}
		}
	}

	// ------------------------------------------------------------------------
	// For Testing
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 * 
	 * @return The ID of the pending of completed checkpoint. 
	 */
	public long getCurrentCheckpointId() {
		return this.currentCheckpointId;
	}
	
	// ------------------------------------------------------------------------
	// Utilities 
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("last checkpoint: %d, current barriers: %d, closed channels: %d",
				currentCheckpointId, numBarriersReceived, numClosedChannels);
	}
	
	// -------------------------------------
	// TEMP HACK for debugging
	
	public void run() {
		while (printerRunning) {
			try {
				Thread.sleep(5000);
			}
			catch (InterruptedException e) {
				// ignore
			}
			
			LOG.info("=====================> BARRIER BUFFER: returned buffers: {}, spilled buffers: {}, re-read buffers: {}",
					returnedBuffers, spilledBuffers, reReadBuffers);
		}
	}
}
