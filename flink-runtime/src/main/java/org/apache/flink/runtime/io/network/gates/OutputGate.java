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


package org.apache.flink.runtime.io.network.gates;

import java.io.IOException;
import java.util.List;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.deployment.ChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.channels.OutputChannel;
import org.apache.flink.runtime.jobgraph.JobID;

public class OutputGate extends Gate<IOReadableWritable> {

	private OutputChannel[] channels;

	private boolean closed;
	
	/**
	 * Constructs a new output gate.
	 *
	 * @param jobId the ID of the job this input gate belongs to
	 * @param gateId the ID of the gate
	 * @param index the index assigned to this output gate at the Environment object
	 */
	public OutputGate(JobID jobId, GateID gateId, int index) {
		super(jobId, gateId, index);
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                             Data processing
	// -----------------------------------------------------------------------------------------------------------------

	public void sendBuffer(Buffer buffer, int targetChannel) throws IOException, InterruptedException {
		this.channels[targetChannel].sendBuffer(buffer);
	}

	public void sendEvent(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		this.channels[targetChannel].sendEvent(event);
	}

	public void sendBufferAndEvent(Buffer buffer, AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		this.channels[targetChannel].sendBufferAndEvent(buffer, event);
	}

	public void broadcastBuffer(Buffer buffer) throws IOException, InterruptedException {
		for (int i = 1; i < this.channels.length; i++) {
			channels[i].sendBuffer(buffer.duplicate());
		}
		channels[0].sendBuffer(buffer);
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		for (OutputChannel channel : this.channels) {
			channel.sendEvent(event);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                              Channels
	// -----------------------------------------------------------------------------------------------------------------

	public void initializeChannels(GateDeploymentDescriptor descriptor) {
		List<ChannelDeploymentDescriptor> channelDescr = descriptor.getChannels();
		
		int numChannels = channelDescr.size();
		this.channels = new OutputChannel[numChannels];

		for (int i = 0; i < numChannels; i++) {
			ChannelDeploymentDescriptor channelDescriptor = channelDescr.get(i);

			ChannelID id = channelDescriptor.getOutputChannelID();
			ChannelID connectedId = channelDescriptor.getInputChannelID();

			this.channels[i] = new OutputChannel(this, i, id, connectedId, getChannelType());
		}
	}

	public OutputChannel[] channels() {
		return this.channels;
	}

	public OutputChannel getChannel(int index) {
		return (index < this.channels.length) ? this.channels[index] : null;
	}

	public int getNumChannels() {
		return this.channels.length;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                              Shutdown
	// -----------------------------------------------------------------------------------------------------------------

	public void requestClose() throws IOException, InterruptedException {
		for (OutputChannel channel : this.channels) {
			channel.requestClose();
		}
	}

	@Override
	public boolean isClosed() {
		if (this.closed) {
			return true;
		}
		
		for (OutputChannel channel : this.channels) {
			if (!channel.isClosed()) {
				return false;
			}
		}
		
		this.closed = true;
		return true;
	}
	
	public void waitForGateToBeClosed() throws InterruptedException {
		if (this.closed) {
			return;
		}
		
		for (OutputChannel channel : this.channels) {
			channel.waitForChannelToBeClosed();
		}
		
		this.closed = true;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public boolean isInputGate() {
		return false;
	}

	@Override
	public String toString() {
		return "Output " + super.toString();
	}

	@Override
	public void publishEvent(AbstractEvent event) throws IOException, InterruptedException {
		// replaced by broadcastEvent(AbstractEvent) => TODO will be removed with changes to input side
	}

	@Override
	public void releaseAllChannelResources() {
		// nothing to do for buffer oriented runtime => TODO will be removed with changes to input side
	}
}
