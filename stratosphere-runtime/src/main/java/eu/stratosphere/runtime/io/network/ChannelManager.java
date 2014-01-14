/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.runtime.io.network;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.CancelTaskException;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.runtime.io.channels.Channel;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.runtime.io.channels.InputChannel;
import eu.stratosphere.runtime.io.channels.OutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.AbstractID;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProviderBroker;
import eu.stratosphere.runtime.io.network.bufferprovider.GlobalBufferPool;
import eu.stratosphere.runtime.io.network.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.runtime.io.network.bufferprovider.SerialSingleBufferPool;
import eu.stratosphere.runtime.io.network.envelope.Envelope;
import eu.stratosphere.runtime.io.network.envelope.EnvelopeDispatcher;
import eu.stratosphere.runtime.io.network.envelope.EnvelopeReceiverList;
import eu.stratosphere.runtime.io.gates.GateID;
import eu.stratosphere.runtime.io.gates.InputGate;
import eu.stratosphere.runtime.io.gates.OutputGate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The channel manager sets up the network buffers and dispatches data between channels.
 */
public final class ChannelManager implements EnvelopeDispatcher, BufferProviderBroker {

	private static final Log LOG = LogFactory.getLog(ChannelManager.class);

	private final ChannelLookupProtocol channelLookupService;

	private final InstanceConnectionInfo connectionInfo;

	private final Map<ChannelID, Channel> channels;

	private final Map<AbstractID, LocalBufferPoolOwner> localBuffersPools;

	private final Map<ChannelID, EnvelopeReceiverList> receiverCache;

	private final GlobalBufferPool globalBufferPool;

	private final NetworkConnectionManager networkConnectionManager;
	
	private final InetSocketAddress ourAddress;
	
	private final SerialSingleBufferPool discardingDataPool;

	// -----------------------------------------------------------------------------------------------------------------

	public ChannelManager(ChannelLookupProtocol channelLookupService, InstanceConnectionInfo connectionInfo,
						  int numNetworkBuffers, int networkBufferSize) throws IOException {
		this.channelLookupService = channelLookupService;
		this.connectionInfo = connectionInfo;
		this.globalBufferPool = new GlobalBufferPool(numNetworkBuffers, networkBufferSize);
		this.networkConnectionManager = new NetworkConnectionManager(this, connectionInfo.address(), connectionInfo.dataPort());

		// management data structures
		this.channels = new ConcurrentHashMap<ChannelID, Channel>();
		this.receiverCache = new ConcurrentHashMap<ChannelID, EnvelopeReceiverList>();
		this.localBuffersPools = new ConcurrentHashMap<AbstractID, LocalBufferPoolOwner>();
		
		this.ourAddress = new InetSocketAddress(connectionInfo.address(), connectionInfo.dataPort());
		
		// a special pool if the data is to be discarded
		this.discardingDataPool = new SerialSingleBufferPool(networkBufferSize);
	}

	public void shutdown() {
		this.networkConnectionManager.shutDown();
		this.globalBufferPool.destroy();
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                               Task registration
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Registers the given task with the channel manager.
	 *
	 * @param task the task to be registered
	 * @throws InsufficientResourcesException thrown if not enough buffers available to safely run this task
	 */
	public void register(Task task) throws InsufficientResourcesException {
		// Check if we can safely run this task with the given buffers
		ensureBufferAvailability(task);

		RuntimeEnvironment environment = task.getRuntimeEnvironment();

		// -------------------------------------------------------------------------------------------------------------
		//                                       Register output channels
		// -------------------------------------------------------------------------------------------------------------

		environment.registerGlobalBufferPool(this.globalBufferPool);

		if (this.localBuffersPools.containsKey(task.getVertexID())) {
			throw new IllegalStateException("Vertex " + task.getVertexID() + " has a previous buffer pool owner");
		}

		for (OutputGate gate : environment.outputGates()) {
			// add receiver list hints
			for (OutputChannel channel : gate.channels()) {
				// register envelope dispatcher with the channel
				channel.registerEnvelopeDispatcher(this);

				switch (channel.getChannelType()) {
					case IN_MEMORY:
						addReceiverListHint(channel.getID(), channel.getConnectedId());
						break;
					case NETWORK:
						addReceiverListHint(channel.getConnectedId(), channel.getID());
						break;
				}

				this.channels.put(channel.getID(), channel);
			}
		}

		this.localBuffersPools.put(task.getVertexID(), environment);

		// -------------------------------------------------------------------------------------------------------------
		//                                       Register input channels
		// -------------------------------------------------------------------------------------------------------------

		// register global
		for (InputGate<?> gate : environment.inputGates()) {
			gate.registerGlobalBufferPool(this.globalBufferPool);

			for (int i = 0; i < gate.getNumberOfInputChannels(); i++) {
				InputChannel<? extends IOReadableWritable> channel = gate.getInputChannel(i);
				channel.registerEnvelopeDispatcher(this);

				if (channel.getChannelType() == ChannelType.IN_MEMORY) {
					addReceiverListHint(channel.getID(), channel.getConnectedId());
				}

				this.channels.put(channel.getID(), channel);
			}

			this.localBuffersPools.put(gate.getGateID(), gate);
		}

		// the number of channels per buffers has changed after unregistering the task
		// => redistribute the number of designated buffers of the registered local buffer pools
		redistributeBuffers();
	}

	/**
	 * Unregisters the given task from the channel manager.
	 *
	 * @param vertexId the ID of the task to be unregistered
	 * @param task the task to be unregistered
	 */
	public void unregister(ExecutionVertexID vertexId, Task task) {
		final Environment environment = task.getEnvironment();

		// destroy and remove OUTPUT channels from registered channels and cache
		for (ChannelID id : environment.getOutputChannelIDs()) {
			Channel channel = this.channels.remove(id);
			if (channel != null) {
				channel.destroy();
			}

			this.receiverCache.remove(channel);
		}

		// destroy and remove INPUT channels from registered channels and cache
		for (ChannelID id : environment.getInputChannelIDs()) {
			Channel channel = this.channels.remove(id);
			if (channel != null) {
				channel.destroy();
			}

			this.receiverCache.remove(channel);
		}

		// clear and remove INPUT side buffer pools
		for (GateID id : environment.getInputGateIDs()) {
			LocalBufferPoolOwner bufferPool = this.localBuffersPools.remove(id);
			if (bufferPool != null) {
				bufferPool.clearLocalBufferPool();
			}
		}

		// clear and remove OUTPUT side buffer pool
		LocalBufferPoolOwner bufferPool = this.localBuffersPools.remove(vertexId);
		if (bufferPool != null) {
			bufferPool.clearLocalBufferPool();
		}

		// the number of channels per buffers has changed after unregistering the task
		// => redistribute the number of designated buffers of the registered local buffer pools
		redistributeBuffers();
	}

	/**
	 * Ensures that the channel manager has enough buffers to execute the given task.
	 * <p>
	 * If there is less than one buffer per channel available, an InsufficientResourcesException will be thrown,
	 * because of possible deadlocks. With more then one buffer per channel, deadlock-freedom is guaranteed.
	 *
	 * @param task task to be executed
	 * @throws InsufficientResourcesException thrown if not enough buffers available to execute the task
	 */
	private void ensureBufferAvailability(Task task) throws InsufficientResourcesException {
		Environment env = task.getEnvironment();

		int numBuffers = this.globalBufferPool.numBuffers();
		// existing channels + channels of the task
		int numChannels = this.channels.size() + env.getNumberOfOutputChannels() + env.getNumberOfInputChannels();

		// need at least one buffer per channel
		if (numBuffers / numChannels < 1) {
			String msg = String.format("%s has not enough buffers to safely execute %s (%d buffers missing)",
					this.connectionInfo.hostname(), env.getTaskName(), numChannels - numBuffers);

			throw new InsufficientResourcesException(msg);
		}
	}

	/**
	 * Redistributes the buffers among the registered buffer pools. This method is called after each task registration
	 * and unregistration.
	 * <p>
	 * Every registered buffer pool gets buffers according to its number of channels weighted by the current buffer to
	 * channel ratio.
	 */
	private void redistributeBuffers() {
		if (this.localBuffersPools.isEmpty() | this.channels.size() == 0) {
			return;
		}

		int numBuffers = this.globalBufferPool.numBuffers();
		int numChannels = this.channels.size();

		double buffersPerChannel = numBuffers / (double) numChannels;

		if (buffersPerChannel < 1.0) {
			throw new RuntimeException("System has not enough buffers to execute tasks.");
		}

		// redistribute number of designated buffers per buffer pool
		for (LocalBufferPoolOwner bufferPool : this.localBuffersPools.values()) {
			int numDesignatedBuffers = (int) Math.ceil(buffersPerChannel * bufferPool.getNumberOfChannels());
			bufferPool.setDesignatedNumberOfBuffers(numDesignatedBuffers);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                           Envelope processing
	// -----------------------------------------------------------------------------------------------------------------

	private void releaseEnvelope(Envelope envelope) {
		Buffer buffer = envelope.getBuffer();
		if (buffer != null) {
			buffer.recycleBuffer();
		}
	}

	private void addReceiverListHint(ChannelID source, ChannelID localReceiver) {
		EnvelopeReceiverList receiverList = new EnvelopeReceiverList(localReceiver);

		if (this.receiverCache.put(source, receiverList) != null) {
			LOG.warn("Receiver cache already contained entry for " + source);
		}
	}

	private void addReceiverListHint(ChannelID source, RemoteReceiver remoteReceiver) {
		EnvelopeReceiverList receiverList = new EnvelopeReceiverList(remoteReceiver);

		if (this.receiverCache.put(source, receiverList) != null) {
			LOG.warn("Receiver cache already contained entry for " + source);
		}
	}

	private void generateSenderHint(Envelope envelope, RemoteReceiver receiver) {
		Channel channel = this.channels.get(envelope.getSource());
		if (channel == null) {
			LOG.error("Cannot find channel for channel ID " + envelope.getSource());
			return;
		}

		// Only generate sender hints for output channels
		if (channel.isInputChannel()) {
			return;
		}

		final ChannelID targetChannelID = channel.getConnectedId();
		final int connectionIndex = receiver.getConnectionIndex();

		final RemoteReceiver ourAddress = new RemoteReceiver(this.ourAddress, connectionIndex);
		final Envelope senderHint = SenderHintEvent.createEnvelopeWithEvent(envelope, targetChannelID, ourAddress);

		this.networkConnectionManager.queueEnvelopeForTransfer(receiver, senderHint);
	}

	/**
	 * Returns the list of receivers for transfer envelopes produced by the channel with the given source channel ID.
	 *
	 * @param jobID
	 *        the ID of the job the given channel ID belongs to
	 * @param sourceChannelID
	 *        the source channel ID for which the receiver list shall be retrieved
	 * @return the list of receivers or <code>null</code> if the receiver could not be determined
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private EnvelopeReceiverList getReceiverList(JobID jobID, ChannelID sourceChannelID, boolean reportException) throws IOException {
		EnvelopeReceiverList receiverList = this.receiverCache.get(sourceChannelID);

		if (receiverList != null) {
			return receiverList;
		}

		while (true) {
			ConnectionInfoLookupResponse lookupResponse;
			synchronized (this.channelLookupService) {
				lookupResponse = this.channelLookupService.lookupConnectionInfo(this.connectionInfo, jobID, sourceChannelID);
			}

			if (lookupResponse.receiverReady()) {
				receiverList = new EnvelopeReceiverList(lookupResponse);
				break;
			}
			else if (lookupResponse.receiverNotReady()) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					if (reportException) {
						throw new IOException("Lookup was interrupted.");
					} else {
						return null;
					}
				}
			}
			else if (lookupResponse.isJobAborting()) {
				if (reportException) {
					throw new CancelTaskException();
				} else {
					return null;
				}
			}
			else if (lookupResponse.receiverNotFound()) {
				if (reportException) {
					throw new IOException("Could not find the receiver for Job " + jobID + ", channel with source id " + sourceChannelID);
				} else {
					return null;
				}
			}
			else {
				throw new IllegalStateException("Unrecognized response to channel lookup.");
			}
		}

		this.receiverCache.put(sourceChannelID, receiverList);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Receivers for source channel ID " + sourceChannelID + " at task manager " + this.connectionInfo +
				": " + receiverList);
		}

		return receiverList;
	}

	/**
	 * Invalidates the entries identified by the given channel IDs from the receiver lookup cache.
	 *
	 * @param channelIDs channel IDs for entries to invalidate
	 */
	public void invalidateLookupCacheEntries(Set<ChannelID> channelIDs) {
		for (ChannelID id : channelIDs) {
			this.receiverCache.remove(id);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                       EnvelopeDispatcher methods
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public void dispatchFromOutputChannel(Envelope envelope) throws IOException, InterruptedException {
		EnvelopeReceiverList receiverList = getReceiverListForEnvelope(envelope, true);

		Buffer srcBuffer = envelope.getBuffer();
		Buffer destBuffer = null;
		
		boolean success = false;
		
		try {
			if (receiverList.hasLocalReceiver()) {
				ChannelID receiver = receiverList.getLocalReceiver();
				Channel channel = this.channels.get(receiver);

				if (channel == null) {
					throw new LocalReceiverCancelledException(receiver);
				}

				if (!channel.isInputChannel()) {
					throw new IOException("Local receiver " + receiver + " is not an input channel.");
				}

				InputChannel<?> inputChannel = (InputChannel<?>) channel;
				
				// copy the buffer into the memory space of the receiver 
				if (srcBuffer != null) {
					try {
						destBuffer = inputChannel.requestBufferBlocking(srcBuffer.size());
					} catch (InterruptedException e) {
						throw new IOException(e.getMessage());
					}
					
					srcBuffer.copyToBuffer(destBuffer);
					envelope.setBuffer(destBuffer);
					srcBuffer.recycleBuffer();
				}
				
				inputChannel.queueEnvelope(envelope);
				success = true;
			}
			else if (receiverList.hasRemoteReceiver()) {
				RemoteReceiver remoteReceiver = receiverList.getRemoteReceiver();

				// Generate sender hint before sending the first envelope over the network
				if (envelope.getSequenceNumber() == 0) {
					generateSenderHint(envelope, remoteReceiver);
				}

				this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, envelope);
				success = true;
			}
		} finally {
			if (!success) {
				if (srcBuffer != null) {
					srcBuffer.recycleBuffer();
				}
				if (destBuffer != null) {
					destBuffer.recycleBuffer();
				}
			}
		}
	}

	@Override
	public void dispatchFromInputChannel(Envelope envelope) throws IOException, InterruptedException {
		// this method sends only events back from input channels to output channels
		// sanity check that we have no buffer
		if (envelope.getBuffer() != null) {
			throw new RuntimeException("Error: This method can only process envelopes without buffers.");
		}
		
		EnvelopeReceiverList receiverList = getReceiverListForEnvelope(envelope, true);

		if (receiverList.hasLocalReceiver()) {
			ChannelID receiver = receiverList.getLocalReceiver();
			Channel channel = this.channels.get(receiver);

			if (channel == null) {
				throw new LocalReceiverCancelledException(receiver);
			}

			if (channel.isInputChannel()) {
				throw new IOException("Local receiver " + receiver + " of backward event is not an output channel.");
			}

			OutputChannel outputChannel = (OutputChannel) channel;
			outputChannel.queueEnvelope(envelope);
		}
		else if (receiverList.hasRemoteReceiver()) {
			RemoteReceiver remoteReceiver = receiverList.getRemoteReceiver();

			// Generate sender hint before sending the first envelope over the network
			if (envelope.getSequenceNumber() == 0) {
				generateSenderHint(envelope, remoteReceiver);
			}

			this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, envelope);
		}
	}

	/**
	 * 
	 */
	@Override
	public void dispatchFromNetwork(Envelope envelope) throws IOException, InterruptedException {
		// ========================================================================================
		//  IMPORTANT
		//  
		//  This method is called by the network I/O thread that reads the incoming TCP 
		//  connections. This method must have minimal overhead and not throw exception if
		//  something is wrong with a job or individual transmission, but only when something
		//  is fundamentally broken in the system.
		// ========================================================================================
		
		// the sender hint event is to let the receiver know where exactly the envelope came from.
		// the receiver will cache the sender id and its connection info in its local lookup table
		// that allows the receiver to send envelopes to the sender without first pinging the job manager
		// for the sender's connection info
		
		// Check if the envelope is the special envelope with the sender hint event
		if (SenderHintEvent.isSenderHintEvent(envelope)) {
			// Check if this is the final destination of the sender hint event before adding it
			final SenderHintEvent seh = (SenderHintEvent) envelope.deserializeEvents().get(0);
			if (this.channels.get(seh.getSource()) != null) {
				addReceiverListHint(seh.getSource(), seh.getRemoteReceiver());
				return;
			}
		}
		
		// try and get the receiver list. if we cannot get it anymore, the task has been cleared
		// the code frees the envelope on exception, so we need not to anything
		EnvelopeReceiverList receiverList = getReceiverListForEnvelope(envelope, false);
		if (receiverList == null) {
			// receiver is cancelled and cleaned away
			releaseEnvelope(envelope);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Dropping envelope for cleaned up receiver.");
			}

			return;
		}

		if (!receiverList.hasLocalReceiver() || receiverList.hasRemoteReceiver()) {
			throw new IOException("Bug in network stack: Envelope dispatched from the incoming network pipe has no local receiver or has a remote receiver");
		}

		ChannelID localReceiver = receiverList.getLocalReceiver();
		Channel channel = this.channels.get(localReceiver);
		
		// if the channel is null, it means that receiver has been cleared already (cancelled or failed).
		// release the buffer immediately
		if (channel == null) {
			releaseEnvelope(envelope);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Dropping envelope for cancelled receiver " + localReceiver);
			}
		}
		else {
			channel.queueEnvelope(envelope);
		}
	}

	/**
	 * 
	 * Upon an exception, this method frees the envelope.
	 * 
	 * @param envelope
	 * @return
	 * @throws IOException
	 */
	private final EnvelopeReceiverList getReceiverListForEnvelope(Envelope envelope, boolean reportException) throws IOException {
		try {
			return getReceiverList(envelope.getJobID(), envelope.getSource(), reportException);
		} catch (IOException e) {
			releaseEnvelope(envelope);
			throw e;
		} catch (CancelTaskException e) {
			releaseEnvelope(envelope);
			throw e;
		}
	}
	
	// -----------------------------------------------------------------------------------------------------------------
	//                                       BufferProviderBroker methods
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public BufferProvider getBufferProvider(JobID jobID, ChannelID sourceChannelID) throws IOException {
		EnvelopeReceiverList receiverList = getReceiverList(jobID, sourceChannelID, false);
		
		// check if the receiver is already gone
		if (receiverList == null) {
			return this.discardingDataPool;
		}

		if (!receiverList.hasLocalReceiver() || receiverList.hasRemoteReceiver()) {
			throw new IOException("The destination to be looked up is not a single local endpoint.");
		}
		

		ChannelID localReceiver = receiverList.getLocalReceiver();
		Channel channel = this.channels.get(localReceiver);
		
		if (channel == null) {
			// receiver is already canceled
			return this.discardingDataPool;
		}

		if (!channel.isInputChannel()) {
			throw new IOException("Channel context for local receiver " + localReceiver + " is not an input channel context");
		}

		return (InputChannel<?>) channel;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public void logBufferUtilization() {
		System.out.println("Buffer utilization at " + System.currentTimeMillis());

		System.out.println("\tUnused global buffers: " + this.globalBufferPool.numAvailableBuffers());

		System.out.println("\tLocal buffer pool status:");

		for (LocalBufferPoolOwner bufferPool : this.localBuffersPools.values()) {
			bufferPool.logBufferUtilization();
		}

		this.networkConnectionManager.logBufferUtilization();

		System.out.println("\tIncoming connections:");

		for (Channel channel : this.channels.values()) {
			if (channel.isInputChannel()) {
				((InputChannel<?>) channel).logQueuedEnvelopes();
			}
		}
	}
}
