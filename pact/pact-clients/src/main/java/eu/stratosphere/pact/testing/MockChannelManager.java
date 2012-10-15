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
package eu.stratosphere.pact.testing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.types.Record;

/**
 * @author Arvid Heise
 */
public class MockChannelManager implements TransferEnvelopeDispatcher {
	private static final Log LOG = LogFactory.getLog(MockChannelManager.class);

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
			OutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);
				if (!(outputChannel instanceof AbstractByteBufferedOutputChannel)) {
					LOG.error("Output channel " + outputChannel.getID() + "of job " + environment.getJobID()
						+ " is not a byte buffered output channel, skipping...");
					continue;
				}

				final AbstractByteBufferedOutputChannel<?> bboc = (AbstractByteBufferedOutputChannel<?>) outputChannel;
				MockOutputChannelBroker channelBroker = new MockOutputChannelBroker(bboc, this.transitBufferPool, this);
				bboc.setByteBufferedOutputChannelBroker(channelBroker);
				this.registeredChannels.put(bboc.getID(), channelBroker);
			}
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final InputGate<?> inputGate = environment.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
				if (!(inputChannel instanceof AbstractByteBufferedInputChannel)) {
					LOG.error("Input channel " + inputChannel.getID() + "of job " + environment.getJobID()
						+ " is not a byte buffered input channel, skipping...");
					continue;
				}

				final AbstractByteBufferedInputChannel<?> bbic = (AbstractByteBufferedInputChannel<?>) inputChannel;
				MockInputChannelBroker channelBroker = new MockInputChannelBroker(bbic, this);
				bbic.setInputChannelBroker(channelBroker);
				this.registeredChannels.put(bbic.getID(), channelBroker);
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
	public void processEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) {
		this.processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) {
		this.processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromNetwork(final TransferEnvelope transferEnvelope, boolean freeSourceBuffer) {
		this.processEnvelope(transferEnvelope);
	}

	/**
	 * @param environment
	 */
	public void unregisterChannels(RuntimeEnvironment environment) {
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			OutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);
				if (!(outputChannel instanceof AbstractByteBufferedOutputChannel)) {
					LOG.error("Output channel " + outputChannel.getID() + "of job " + environment.getJobID()
						+ " is not a byte buffered output channel, skipping...");
					continue;
				}

				final AbstractByteBufferedOutputChannel<?> bboc = (AbstractByteBufferedOutputChannel<?>) outputChannel;
				this.registeredChannels.remove(bboc.getID());
			}
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final InputGate<?> inputGate = environment.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
				if (!(inputChannel instanceof AbstractByteBufferedInputChannel)) {
					LOG.error("Input channel " + inputChannel.getID() + "of job " + environment.getJobID()
						+ " is not a byte buffered input channel, skipping...");
					continue;
				}

				final AbstractByteBufferedInputChannel<?> bbic = (AbstractByteBufferedInputChannel<?>) inputChannel;
				MockInputChannelBroker channelBroker = new MockInputChannelBroker(bbic, this);
				bbic.setInputChannelBroker(channelBroker);
				this.registeredChannels.remove(bbic.getID());
			}
		}
	}

}
