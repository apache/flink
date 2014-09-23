/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.io.network.channels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.AbstractTaskEvent;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.gates.OutputGate;
import org.apache.flink.runtime.jobgraph.JobID;

import java.io.IOException;
import java.util.Arrays;


public class OutputChannel extends Channel {

	private static final Logger LOG = LoggerFactory.getLogger(OutputChannel.class);

	private final Object closeLock = new Object();
	
	private final OutputGate outputGate;

	private boolean senderCloseRequested;

	private boolean receiverCloseRequested;

	private int currentSeqNum;

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Creates a new output channel object.
	 *
	 * @param outputGate the output gate this channel is connected to
	 * @param index the index of the channel in the output gate
	 * @param id the ID of the channel
	 * @param connectedId the ID of the channel this channel is connected to
	 * @param type the type of this channel
	 */
	public OutputChannel(OutputGate outputGate, int index, ChannelID id, ChannelID connectedId, ChannelType type) {
		super(index, id, connectedId, type);

		this.outputGate = outputGate;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                           Data processing
	// -----------------------------------------------------------------------------------------------------------------

	public void sendBuffer(Buffer buffer) throws IOException, InterruptedException {
		checkStatus();

		// discard empty buffers
		if (buffer.size() == 0) {
			buffer.recycleBuffer();
			return;
		}

		Envelope envelope = createNextEnvelope();
		envelope.setBuffer(buffer);
		this.envelopeDispatcher.dispatchFromOutputChannel(envelope);
	}

	public void sendEvent(AbstractEvent event) throws IOException, InterruptedException {
		checkStatus();

		Envelope envelope = createNextEnvelope();
		envelope.serializeEventList(Arrays.asList(event));
		this.envelopeDispatcher.dispatchFromOutputChannel(envelope);
	}

	public void sendBufferAndEvent(Buffer buffer, AbstractEvent event) throws IOException, InterruptedException {
		checkStatus();

		Envelope envelope = createNextEnvelope();
		envelope.setBuffer(buffer);
		envelope.serializeEventList(Arrays.asList(event));
		this.envelopeDispatcher.dispatchFromOutputChannel(envelope);
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                          Event processing
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public void queueEnvelope(Envelope envelope) {
		if (envelope.hasBuffer()) {
			throw new IllegalStateException("Envelope for OutputChannel has Buffer attached.");
		}

		for (AbstractEvent event : envelope.deserializeEvents()) {
			if (event.getClass() == ChannelCloseEvent.class) {
				synchronized (this.closeLock) {
					this.receiverCloseRequested = true;
					this.closeLock.notifyAll();
				}
			} 
			else if (event instanceof AbstractTaskEvent) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("OutputChannel received task event: " + event);
				}
				
				this.outputGate.deliverEvent((AbstractTaskEvent) event);
			}
			else {
				throw new RuntimeException("OutputChannel received an event that is neither close nor task event.");
			}
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                              Shutdown
	// -----------------------------------------------------------------------------------------------------------------

	public void requestClose() throws IOException, InterruptedException {
		if (this.senderCloseRequested) {
			return;
		}

		this.senderCloseRequested = true;

		Envelope envelope = createNextEnvelope();
		envelope.serializeEventList(Arrays.asList(new ChannelCloseEvent()));
		this.envelopeDispatcher.dispatchFromOutputChannel(envelope);
	}

	@Override
	public boolean isClosed() {
		return this.senderCloseRequested && this.receiverCloseRequested;
	}
	
	public void waitForChannelToBeClosed() throws InterruptedException {
		synchronized (this.closeLock) {
			while (!this.receiverCloseRequested) {
				this.closeLock.wait(1000);
			}
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public boolean isInputChannel() {
		return false;
	}

	@Override
	public JobID getJobID() {
		return this.outputGate.getJobID();
	}
	
	private void checkStatus() throws IOException {
		if (this.senderCloseRequested) {
			throw new IllegalStateException(String.format("Channel %s already requested to be closed", getID()));
		}
		if (this.receiverCloseRequested) {
			throw new ReceiverAlreadyClosedException();
		}
	}

	private Envelope createNextEnvelope() {
		return new Envelope(this.currentSeqNum++, getJobID(), getID());
	}

	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {
		// TODO remove with pending changes for input side
	}

	@Override
	public void releaseAllResources() {
		// nothing to do for buffer oriented runtime => TODO remove with pending changes for input side
	}

	@Override
	public void destroy() {
		// nothing to do for buffer oriented runtime => TODO remove with pending changes for input side
	}
}
