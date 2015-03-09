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

package org.apache.flink.streaming.io;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.streamvertex.StreamingSuperstep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BarrierBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	private Queue<BufferOrEvent> nonprocessed = new LinkedList<BufferOrEvent>();
	private Queue<BufferOrEvent> blockedNonprocessed = new LinkedList<BufferOrEvent>();

	private Set<Integer> blockedChannels = new HashSet<Integer>();
	private int totalNumberOfInputChannels;

	private StreamingSuperstep currentSuperstep;
	private boolean superstepStarted;

	private AbstractReader reader;

	private InputGate inputGate;

	public BarrierBuffer(InputGate inputGate, AbstractReader reader) {
		this.inputGate = inputGate;
		totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.reader = reader;
	}

	/**
	 * Starts the next superstep
	 * 
	 * @param superstep
	 *            The next superstep
	 */
	protected void startSuperstep(StreamingSuperstep superstep) {
		this.currentSuperstep = superstep;
		this.superstepStarted = true;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Superstep started with id: " + superstep.getId());
		}
	}

	/**
	 * Buffers a bufferOrEvent received from a blocked channel
	 * 
	 * @param bufferOrEvent
	 *            bufferOrEvent to buffer
	 */
	protected void store(BufferOrEvent bufferOrEvent) {
		nonprocessed.add(bufferOrEvent);
	}

	/**
	 * Get then next non-blocked non-processed BufferOrEvent. Returns null if
	 * not available.
	 */
	protected BufferOrEvent getNonProcessed() {
		BufferOrEvent nextNonprocessed;
		while ((nextNonprocessed = nonprocessed.poll()) != null) {
			if (isBlocked(nextNonprocessed.getChannelIndex())) {
				blockedNonprocessed.add(nextNonprocessed);
			} else {
				return nextNonprocessed;
			}
		}
		return null;
	}

	/**
	 * Checks whether a given channel index is blocked for this inputgate
	 * 
	 * @param channelIndex
	 *            The channel index to check
	 */
	protected boolean isBlocked(int channelIndex) {
		return blockedChannels.contains(channelIndex);
	}

	/**
	 * Checks whether all channels are blocked meaning that barriers are
	 * received from all channels
	 */
	protected boolean isAllBlocked() {
		return blockedChannels.size() == totalNumberOfInputChannels;
	}

	/**
	 * Returns the next non-blocked BufferOrEvent. This is a blocking operator.
	 */
	public BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException {
		// If there are non-processed buffers from the previously blocked ones,
		// we get the next
		BufferOrEvent bufferOrEvent = getNonProcessed();

		if (bufferOrEvent != null) {
			return bufferOrEvent;
		} else {
			// If no non-processed, get new from input
			while (true) {
				// We read the next buffer from the inputgate
				bufferOrEvent = inputGate.getNextBufferOrEvent();
				if (isBlocked(bufferOrEvent.getChannelIndex())) {
					// If channel blocked we just store it
					blockedNonprocessed.add(bufferOrEvent);
				} else {
					return bufferOrEvent;
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
	protected void blockChannel(int channelIndex) {
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
	protected void releaseBlocks() {
		if (!nonprocessed.isEmpty()) {
			// sanity check
			throw new RuntimeException("Error in barrier buffer logic");
		}
		nonprocessed = blockedNonprocessed;
		blockedNonprocessed = new LinkedList<BufferOrEvent>();
		blockedChannels.clear();
		superstepStarted = false;
		if (LOG.isDebugEnabled()) {
			LOG.debug("All barriers received, blocks released");
		}
	}

	/**
	 * Method that is executed once the barrier has been received from all
	 * channels.
	 */
	protected void actOnAllBlocked() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Publishing barrier to the vertex");
		}
		reader.publish(currentSuperstep);
		releaseBlocks();
	}

	/**
	 * Processes a streaming superstep event
	 * 
	 * @param bufferOrEvent
	 *            The BufferOrEvent containing the event
	 */
	public void processSuperstep(BufferOrEvent bufferOrEvent) {
		StreamingSuperstep superstep = (StreamingSuperstep) bufferOrEvent.getEvent();
		if (!superstepStarted) {
			startSuperstep(superstep);
		}
		blockChannel(bufferOrEvent.getChannelIndex());
	}

}