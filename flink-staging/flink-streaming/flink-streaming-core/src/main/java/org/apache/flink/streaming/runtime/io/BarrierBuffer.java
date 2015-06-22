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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.runtime.tasks.CheckpointBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The barrier buffer is responsible for implementing the blocking behaviour described
 * here: {@link CheckpointBarrier}.
 *
 * <p>
 * To avoid back-pressuring the
 * readers, we buffer up the new data received from the blocked channels until
 * the blocks are released.
 */
public class BarrierBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	private Queue<SpillingBufferOrEvent> nonProcessed = new LinkedList<SpillingBufferOrEvent>();
	private Queue<SpillingBufferOrEvent> blockedNonProcessed = new LinkedList<SpillingBufferOrEvent>();

	private Set<Integer> blockedChannels = new HashSet<Integer>();
	private int totalNumberOfInputChannels;

	private CheckpointBarrier currentBarrier;

	private AbstractReader reader;

	private InputGate inputGate;

	private SpillReader spillReader;
	private BufferSpiller bufferSpiller;

	private boolean inputFinished = false;

	private BufferOrEvent endOfStreamEvent = null;

	private long lastCheckpointId = Long.MIN_VALUE;

	public BarrierBuffer(InputGate inputGate, AbstractReader reader) {
		this.inputGate = inputGate;
		totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.reader = reader;
		try {
			this.bufferSpiller = new BufferSpiller();
			this.spillReader = new SpillReader();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Get then next non-blocked non-processed {@link BufferOrEvent}. Returns null if
	 * none available.
	 * 
	 * @throws IOException
	 */
	private BufferOrEvent getNonProcessed() throws IOException {
		SpillingBufferOrEvent nextNonProcessed;

		while ((nextNonProcessed = nonProcessed.poll()) != null) {
			BufferOrEvent boe = nextNonProcessed.getBufferOrEvent();
			if (isBlocked(boe.getChannelIndex())) {
				blockedNonProcessed.add(new SpillingBufferOrEvent(boe, bufferSpiller, spillReader));
			} else {
				return boe;
			}
		}

		return null;
	}

	/**
	 * Checks whether the channel with the given index is blocked.
	 * 
	 * @param channelIndex The channel index to check
	 */
	private boolean isBlocked(int channelIndex) {
		return blockedChannels.contains(channelIndex);
	}

	/**
	 * Checks whether all channels are blocked meaning that barriers have been
	 * received from all channels
	 */
	private boolean isAllBlocked() {
		return blockedChannels.size() == totalNumberOfInputChannels;
	}

	/**
	 * Returns the next non-blocked {@link BufferOrEvent}. This is a blocking operator.
	 */
	public BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException {
		// If there are non-processed buffers from the previously blocked ones,
		// we get the next
		BufferOrEvent bufferOrEvent = getNonProcessed();

		if (bufferOrEvent != null) {
			return bufferOrEvent;
		} else if (blockedNonProcessed.isEmpty() && inputFinished) {
			return endOfStreamEvent;
		} else {
			// If no non-processed, get new from input
			while (true) {
				if (!inputFinished) {
					// We read the next buffer from the inputgate
					bufferOrEvent = inputGate.getNextBufferOrEvent();

					if (!bufferOrEvent.isBuffer()
							&& bufferOrEvent.getEvent() instanceof EndOfPartitionEvent) {
						if (inputGate.isFinished()) {
							// store the event for later if the channel is
							// closed
							endOfStreamEvent = bufferOrEvent;
							inputFinished = true;
						}

					} else {
						if (isBlocked(bufferOrEvent.getChannelIndex())) {
							// If channel blocked we just store it
							blockedNonProcessed.add(new SpillingBufferOrEvent(bufferOrEvent,
									bufferSpiller, spillReader));
						} else {
							return bufferOrEvent;
						}
					}
				} else {
					actOnAllBlocked();
					return getNextNonBlocked();
				}
			}
		}
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 * 
	 * @param channelIndex
	 *            The channel index to block.
	 */
	private void blockChannel(int channelIndex) {
		if (!blockedChannels.contains(channelIndex)) {
			blockedChannels.add(channelIndex);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Channel blocked with index: " + channelIndex);
			}
			if (isAllBlocked()) {
				actOnAllBlocked();
			}

		} else {
			throw new RuntimeException("Tried to block an already blocked channel");
		}
	}

	/**
	 * Releases the blocks on all channels.
	 */
	private void releaseBlocks() {
		if (!nonProcessed.isEmpty()) {
			// sanity check
			throw new RuntimeException("Error in barrier buffer logic");
		}
		nonProcessed = blockedNonProcessed;
		blockedNonProcessed = new LinkedList<SpillingBufferOrEvent>();

		try {
			spillReader.setSpillFile(bufferSpiller.getSpillFile());
			bufferSpiller.resetSpillFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		blockedChannels.clear();
		currentBarrier = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("All barriers received, blocks released");
		}
	}

	/**
	 * Method that is executed once the barrier has been received from all
	 * channels.
	 */
	private void actOnAllBlocked() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Publishing barrier to the vertex");
		}

		if (currentBarrier != null && !inputFinished) {
			reader.publish(currentBarrier);
			lastCheckpointId = currentBarrier.getId();
		}

		releaseBlocks();
	}

	/**
	 * Processes one {@link org.apache.flink.streaming.runtime.tasks.CheckpointBarrier}
	 * 
	 * @param bufferOrEvent The {@link BufferOrEvent} containing the checkpoint barrier
	 */
	public void processBarrier(BufferOrEvent bufferOrEvent) {
		CheckpointBarrier receivedBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();

		if (receivedBarrier.getId() < lastCheckpointId) {
			// a barrier from an old checkpoint, ignore these
			return;
		}

		if (currentBarrier == null) {
			this.currentBarrier = receivedBarrier;
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checkpoint barrier received start waiting for checkpoint: {}", receivedBarrier);
			}
		} else if (receivedBarrier.getId() > currentBarrier.getId()) {
			// we have a barrier from a more recent checkpoint, free all locks and start with
			// this newer checkpoint
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checkpoint barrier received while waiting on checkpoint {}. Restarting waiting with checkpoint {}: ", currentBarrier, receivedBarrier);
			}
			releaseBlocks();
			currentBarrier = receivedBarrier;

		}
		blockChannel(bufferOrEvent.getChannelIndex());
	}

	public void cleanup() throws IOException {
		bufferSpiller.close();
		File spillfile1 = bufferSpiller.getSpillFile();
		if (spillfile1 != null) {
			spillfile1.delete();
		}

		spillReader.close();
		File spillfile2 = spillReader.getSpillFile();
		if (spillfile2 != null) {
			spillfile2.delete();
		}
	}

	public String toString() {
		return nonProcessed.toString() + blockedNonProcessed.toString();
	}

	public boolean isEmpty() {
		return nonProcessed.isEmpty() && blockedNonProcessed.isEmpty();
	}

}
