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
package eu.stratosphere.pact.testing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.routing.RoutingLayer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.types.Record;

/**
 * @author Arvid Heise
 */
public class MockChannelManager implements RoutingLayer {

	private LocalBufferPool transitBufferPool;

	private Map<ChannelID, MockChannelBroker> registeredChannels =
		new ConcurrentHashMap<ChannelID, MockChannelBroker>();

	/**
	 * Initializes MockChannelManager.
	 */
	public MockChannelManager() {
		this.transitBufferPool = new LocalBufferPool(128, true);
	}

	protected synchronized void registerChannels(RuntimeEnvironment environment) {
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final RuntimeOutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);

				MockOutputChannelBroker channelBroker = new MockOutputChannelBroker(outputChannel, this.transitBufferPool, this);
				outputChannel.setByteBufferedOutputChannelBroker(channelBroker);
				this.registeredChannels.put(outputChannel.getID(), channelBroker);
			}
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final RuntimeInputGate<?> inputGate = environment.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
				
				MockInputChannelBroker channelBroker = new MockInputChannelBroker(inputChannel, this);
				inputChannel.setInputChannelBroker(channelBroker);
				this.registeredChannels.put(inputChannel.getID(), channelBroker);
			}
		}
	}

	private synchronized void processEnvelope(final TransferEnvelope transferEnvelope) {
		try {
			AbstractChannel sourceChannel = this.registeredChannels.get(transferEnvelope.getSource()).getChannel();

			final ChannelID localReceiver = sourceChannel.getConnectedChannelID();

			final MockChannelBroker channel = this.registeredChannels.get(localReceiver);

			// if(transferEnvelope.getBuffer() == null)
			if (channel == null)
				System.err.println("Unknown channel " + localReceiver);
			else
				channel.queueTransferEnvelope(transferEnvelope);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void routeEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) {
		this.processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void routeEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) {
		this.processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void routeEnvelopeFromNetwork(final TransferEnvelope transferEnvelope, boolean freeSourceBuffer) {
		this.processEnvelope(transferEnvelope);
	}

	/**
	 * @param environment
	 */
	public void unregisterChannels(RuntimeEnvironment environment) {
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			RuntimeOutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);
				this.registeredChannels.remove(outputChannel.getID());
			}
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final RuntimeInputGate<?> inputGate = environment.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
				this.registeredChannels.remove(inputChannel.getID());
			}
		}
	}

}
