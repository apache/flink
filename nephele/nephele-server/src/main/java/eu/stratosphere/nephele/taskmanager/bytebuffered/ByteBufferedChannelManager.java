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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.CheckpointDecision;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProviderBroker;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.transferenvelope.SpillingQueue;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeReceiverList;
import eu.stratosphere.nephele.util.StringUtils;

public final class ByteBufferedChannelManager implements TransferEnvelopeDispatcher, BufferProviderBroker {

	/**
	 * The log object used to report problems and errors.
	 */
	private static final Log LOG = LogFactory.getLog(ByteBufferedChannelManager.class);

	private static final boolean DEFAULT_ALLOW_SENDER_SIDE_SPILLING = false;

	private static final boolean DEFAULT_MERGE_SPILLED_BUFFERS = true;

	private final Map<ChannelID, ChannelContext> registeredChannels = new ConcurrentHashMap<ChannelID, ChannelContext>();

	private final Map<AbstractID, LocalBufferPoolOwner> localBufferPoolOwner = new ConcurrentHashMap<AbstractID, LocalBufferPoolOwner>();

	private final Map<ExecutionVertexID, TaskContext> tasksWithUndecidedCheckpoints = new ConcurrentHashMap<ExecutionVertexID, TaskContext>();

	private final NetworkConnectionManager networkConnectionManager;

	private final ChannelLookupProtocol channelLookupService;

	private final InstanceConnectionInfo localConnectionInfo;

	private final LocalBufferPool transitBufferPool;

	private final boolean allowSenderSideSpilling;

	private final boolean mergeSpilledBuffers;

	private final boolean multicastEnabled = true;

	/**
	 * This map caches transfer envelope receiver lists.
	 */
	private final Map<ChannelID, TransferEnvelopeReceiverList> receiverCache = new ConcurrentHashMap<ChannelID, TransferEnvelopeReceiverList>();

	public ByteBufferedChannelManager(final ChannelLookupProtocol channelLookupService,
			final InstanceConnectionInfo localInstanceConnectionInfo)
			throws IOException {

		this.channelLookupService = channelLookupService;

		this.localConnectionInfo = localInstanceConnectionInfo;

		// Initialized the file buffer manager
		FileBufferManager.getInstance();

		// Initialize the global buffer pool
		GlobalBufferPool.getInstance();

		// Initialize the transit buffer pool
		this.transitBufferPool = new LocalBufferPool(128, true);

		this.networkConnectionManager = new NetworkConnectionManager(this,
			localInstanceConnectionInfo.getAddress(), localInstanceConnectionInfo.getDataPort());

		this.allowSenderSideSpilling = GlobalConfiguration.getBoolean("channel.network.allowSenderSideSpilling",
			DEFAULT_ALLOW_SENDER_SIDE_SPILLING);

		this.mergeSpilledBuffers = GlobalConfiguration.getBoolean("channel.network.mergeSpilledBuffers",
			DEFAULT_MERGE_SPILLED_BUFFERS);

		LOG.info("Initialized byte buffered channel manager with sender-side spilling "
			+ (this.allowSenderSideSpilling ? "enabled" : "disabled")
			+ (this.mergeSpilledBuffers ? " and spilled buffer merging enabled" : ""));
	}

	/**
	 * Registers the given task with the byte buffered channel manager.
	 * 
	 * @param task
	 *        the task to be registered
	 * @param the
	 *        set of output channels which are initially active
	 */
	public void register(final Task task, final Set<ChannelID> activeOutputChannels) {

		final RuntimeEnvironment environment = task.getEnvironment();

		final TaskContext taskContext = new TaskContext(task, this, this.tasksWithUndecidedCheckpoints);

		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final RuntimeOutputGate<?> outputGate = environment.getOutputGate(i);
			final OutputGateContext outputGateContext = new OutputGateContext(taskContext, outputGate.getChannelType(),
				outputGate.getIndex());
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);
				if (!(outputChannel instanceof AbstractByteBufferedOutputChannel)) {
					LOG.error("Output channel " + outputChannel.getID() + "of job " + environment.getJobID()
						+ " is not a byte buffered output channel, skipping...");
					continue;
				}

				final AbstractByteBufferedOutputChannel<?> bboc = (AbstractByteBufferedOutputChannel<?>) outputChannel;

				if (this.registeredChannels.containsKey(bboc.getID())) {
					LOG.error("Byte buffered output channel " + bboc.getID() + " is already registered");
					continue;
				}

				// Add routing entry to receiver cache to reduce latency
				if (bboc.getType() == ChannelType.INMEMORY) {
					addReceiverListHint(bboc);
				}

				final boolean isActive = activeOutputChannels.contains(bboc.getID());

				LOG.info("Registering byte buffered output channel " + bboc.getID() + " ("
					+ (isActive ? "active" : "inactive") + ")");

				final OutputChannelContext outputChannelContext = new OutputChannelContext(outputGateContext, bboc,
					isActive, this.mergeSpilledBuffers);
				this.registeredChannels.put(bboc.getID(), outputChannelContext);
			}
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final RuntimeInputGate<?> inputGate = environment.getInputGate(i);
			final InputGateContext inputGateContext = new InputGateContext(inputGate.getNumberOfInputChannels());
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
				if (!(inputChannel instanceof AbstractByteBufferedInputChannel)) {
					LOG.error("Input channel " + inputChannel.getID() + "of job " + environment.getJobID()
						+ " is not a byte buffered input channel, skipping...");
					continue;
				}

				final AbstractByteBufferedInputChannel<?> bbic = (AbstractByteBufferedInputChannel<?>) inputChannel;

				if (this.registeredChannels.containsKey(bbic.getID())) {
					LOG.error("Byte buffered input channel " + bbic.getID() + " is already registered");
					continue;
				}

				// Add routing entry to receiver cache to reduce latency
				if (bbic.getType() == ChannelType.INMEMORY) {
					addReceiverListHint(bbic);
				}

				LOG.info("Registering byte buffered input channel " + bbic.getID());

				final InputChannelContext inputChannelContext = new InputChannelContext(inputGateContext, this,
					bbic);
				this.registeredChannels.put(bbic.getID(), inputChannelContext);
			}

			// Add input gate context to set of local buffer pool owner
			this.localBufferPoolOwner.put(inputGate.getGateID(), inputGateContext);
		}

		this.localBufferPoolOwner.put(task.getVertexID(), taskContext);

		redistributeGlobalBuffers();
	}

	/**
	 * Unregisters the given task from the byte buffered channel manager.
	 * 
	 * @param vertexID
	 *        the ID of the task to be unregistered
	 * @param task
	 *        the task to be unregistered
	 */
	public void unregister(final ExecutionVertexID vertexID, final Task task) {

		final RuntimeEnvironment environment = task.getEnvironment();

		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final RuntimeOutputGate<?> outputGate = environment.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);
				this.registeredChannels.remove(outputChannel.getID());
				this.receiverCache.remove(outputChannel.getID());
			}
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final RuntimeInputGate<?> inputGate = environment.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
				final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
				this.registeredChannels.remove(inputChannel.getID());
				this.receiverCache.remove(inputChannel.getID());
			}

			final LocalBufferPoolOwner owner = this.localBufferPoolOwner.remove(inputGate.getGateID());
			if (owner == null) {
				LOG.error("Cannot find local buffer pool owner for input gate " + inputGate.getGateID());
			} else {
				owner.clearLocalBufferPool();
			}
		}

		final LocalBufferPoolOwner owner = this.localBufferPoolOwner.remove(vertexID);
		if (owner == null) {
			LOG.error("Cannot find local buffer pool owner for vertex ID" + vertexID);
		} else {
			owner.clearLocalBufferPool();
		}

		redistributeGlobalBuffers();
	}

	/**
	 * Shuts down the byte buffered channel manager and stops all its internal processes.
	 */
	public void shutdown() {

		this.networkConnectionManager.shutDown();
	}

	public void reportIOExceptionForAllInputChannels(IOException ioe) {

		final Iterator<ChannelContext> it = this.registeredChannels.values().iterator();

		while (it.hasNext()) {

			final ChannelContext channelContext = it.next();
			if (channelContext.isInputChannel()) {
				channelContext.reportIOException(ioe);
			}
		}
	}

	public void reportIOExceptionForOutputChannel(ChannelID sourceChannelID, IOException ioe) {

		final ChannelContext channelContext = this.registeredChannels.get(sourceChannelID);

		if (channelContext == null) {
			LOG.error("Cannot find network output channel with ID " + sourceChannelID);
			return;
		}

		if (channelContext.isInputChannel()) {
			channelContext.reportIOException(ioe);
		}
	}

	public NetworkConnectionManager getNetworkConnectionManager() {

		return this.networkConnectionManager;
	}

	private void processEnvelope(final TransferEnvelope transferEnvelope, final boolean freeSourceBuffer)
			throws IOException, InterruptedException {

		final TransferEnvelopeReceiverList receiverList = getReceiverList(transferEnvelope.getJobID(),
			transferEnvelope.getSource());

		if (receiverList == null) {
			throw new IOException("Transfer envelope " + transferEnvelope.getSequenceNumber() + " from source channel "
				+ transferEnvelope.getSource() + " has not have a receiver list");
		}

		// This envelope is known to have either no buffer or an memory-based input buffer
		if (transferEnvelope.getBuffer() == null) {
			processEnvelopeEnvelopeWithoutBuffer(transferEnvelope, receiverList);
		} else {
			processEnvelopeWithBuffer(transferEnvelope, receiverList, freeSourceBuffer);
		}
	}

	private void processEnvelopeWithBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList, final boolean freeSourceBuffer)
			throws IOException, InterruptedException {

		// Handle the most common (unicast) case first
		if (!freeSourceBuffer) {

			final List<ChannelID> localReceivers = receiverList.getLocalReceivers();
			if (localReceivers.size() != 1) {
				throw new IOException("Expected receiver list to have exactly one element");
			}

			final ChannelID localReceiver = localReceivers.get(0);

			final ChannelContext cc = this.registeredChannels.get(localReceiver);
			if (cc == null) {
				throw new IOException("Cannot find channel context for local receiver " + localReceiver);
			}

			if (!cc.isInputChannel()) {
				throw new IOException("Local receiver " + localReceiver
					+ " is not an input channel, but is supposed to accept a buffer");
			}

			cc.queueTransferEnvelope(transferEnvelope);

			return;
		}

		// This is the in-memory or multicast case
		final Buffer srcBuffer = transferEnvelope.getBuffer();

		if (receiverList.hasLocalReceivers()) {

			final List<ChannelID> localReceivers = receiverList.getLocalReceivers();

			for (final ChannelID localReceiver : localReceivers) {

				final ChannelContext cc = this.registeredChannels.get(localReceiver);
				if (cc == null) {
					throw new IOException("Cannot find channel context for local receiver " + localReceiver);
				}

				if (!cc.isInputChannel()) {
					throw new IOException("Local receiver " + localReceiver
						+ " is not an input channel, but is supposed to accept a buffer");
				}

				final InputChannelContext inputChannelContext = (InputChannelContext) cc;
				final Buffer destBuffer = inputChannelContext.requestEmptyBufferBlocking(srcBuffer.size());
				srcBuffer.copyToBuffer(destBuffer);
				// TODO: See if we can save one duplicate step here
				final TransferEnvelope dup = transferEnvelope.duplicateWithoutBuffer();
				dup.setBuffer(destBuffer);
				inputChannelContext.queueTransferEnvelope(dup);
			}
		}

		if (receiverList.hasRemoteReceivers()) {

			final List<InetSocketAddress> remoteReceivers = receiverList.getRemoteReceivers();
			for (final InetSocketAddress remoteReceiver : remoteReceivers) {

				this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, transferEnvelope.duplicate());
			}
		}

		// Recycle the source buffer
		srcBuffer.recycleBuffer();
	}

	private boolean processEnvelopeEnvelopeWithoutBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList) {

		// No need to copy anything
		final Iterator<ChannelID> localIt = receiverList.getLocalReceivers().iterator();

		while (localIt.hasNext()) {

			final ChannelID localReceiver = localIt.next();

			final ChannelContext channelContext = this.registeredChannels.get(localReceiver);
			if (channelContext == null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Cannot find local receiver " + localReceiver + " for job "
						+ transferEnvelope.getJobID());
				}
				continue;
			}
			channelContext.queueTransferEnvelope(transferEnvelope);
		}

		final Iterator<InetSocketAddress> remoteIt = receiverList.getRemoteReceivers().iterator();

		while (remoteIt.hasNext()) {

			final InetSocketAddress remoteReceiver = remoteIt.next();
			this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, transferEnvelope);
		}

		return true;
	}

	private void addReceiverListHint(final AbstractChannel channel) {

		TransferEnvelopeReceiverList receiverList = new TransferEnvelopeReceiverList(channel);

		if (this.receiverCache.put(channel.getID(), receiverList) != null) {
			LOG.warn("Receiver cache already contained entry for " + channel.getID());
		}
	}

	private TransferEnvelopeReceiverList getReceiverList(final JobID jobID, final ChannelID sourceChannelID)
			throws IOException, InterruptedException {

		TransferEnvelopeReceiverList receiverList = this.receiverCache.get(sourceChannelID);
		if (receiverList == null) {

			while (true) {

				final ConnectionInfoLookupResponse lookupResponse = this.channelLookupService.lookupConnectionInfo(
					this.localConnectionInfo, jobID, sourceChannelID);

				if (lookupResponse.receiverNotFound()) {
					throw new IOException("Cannot find task(s) waiting for data from source channel with ID "
						+ sourceChannelID);
				}

				if (lookupResponse.receiverNotReady()) {
					Thread.sleep(500);
					continue;
				}

				if (lookupResponse.receiverReady()) {
					receiverList = new TransferEnvelopeReceiverList(lookupResponse);
					break;
				}
			}

			if (receiverList == null) {
				LOG.error("Receiver list is null for source channel ID " + sourceChannelID);
			} else {
				this.receiverCache.put(sourceChannelID, receiverList);

				if (LOG.isDebugEnabled()) {

					final StringBuilder sb = new StringBuilder();
					sb.append("Receiver list for source channel ID " + sourceChannelID + " at task manager "
						+ this.localConnectionInfo + "\n");

					if (receiverList.hasLocalReceivers()) {
						sb.append("\tLocal receivers:\n");
						final Iterator<ChannelID> it = receiverList.getLocalReceivers().iterator();
						while (it.hasNext()) {
							sb.append("\t\t" + it.next() + "\n");
						}
					}

					if (receiverList.hasRemoteReceivers()) {
						sb.append("Remote receivers:\n");
						final Iterator<InetSocketAddress> it = receiverList.getRemoteReceivers().iterator();
						while (it.hasNext()) {
							sb.append("\t\t" + it.next() + "\n");
						}
					}

					LOG.debug(sb.toString());
				}
			}
		}

		return receiverList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope, true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromNetwork(final TransferEnvelope transferEnvelope, boolean freeSourceBuffer)
			throws IOException {

		try {
			processEnvelope(transferEnvelope, freeSourceBuffer);
		} catch (InterruptedException e) {
			LOG.error("Caught unexpected interrupted exception: " + StringUtils.stringifyException(e));
		}
	}

	/**
	 * Triggers the byte buffer channel manager write the current utilization of its read and write buffers to the logs.
	 * This method is primarily for debugging purposes.
	 */
	public void logBufferUtilization() {

		System.out.println("Buffer utilization for at " + System.currentTimeMillis());

		System.out.println("\tUnused global buffers: " + GlobalBufferPool.getInstance().getCurrentNumberOfBuffers());

		System.out.println("\tLocal buffer pool status:");

		final Iterator<LocalBufferPoolOwner> it = this.localBufferPoolOwner.values().iterator();
		while (it.hasNext()) {
			it.next().logBufferUtilization();
		}

		this.networkConnectionManager.logBufferUtilization();

		System.out.println("\tIncoming connections:");

		final Iterator<Map.Entry<ChannelID, ChannelContext>> it2 = this.registeredChannels.entrySet()
			.iterator();

		while (it2.hasNext()) {

			final Map.Entry<ChannelID, ChannelContext> entry = it2.next();
			final ChannelContext context = entry.getValue();
			if (context.isInputChannel()) {

				final InputChannelContext inputChannelContext = (InputChannelContext) context;
				final int numberOfQueuedEnvelopes = inputChannelContext.getNumberOfQueuedEnvelopes();
				final int numberOfQueuedMemoryBuffers = inputChannelContext.getNumberOfQueuedMemoryBuffers();

				System.out.println("\t\t" + entry.getKey() + ": " + numberOfQueuedMemoryBuffers + " ("
					+ numberOfQueuedEnvelopes + ")");
			}
		}
	}

	@Override
	public BufferProvider getBufferProvider(final JobID jobID, final ChannelID sourceChannelID) throws IOException,
			InterruptedException {

		final TransferEnvelopeReceiverList receiverList = getReceiverList(jobID, sourceChannelID);

		if (receiverList.hasLocalReceivers() && !receiverList.hasRemoteReceivers()) {

			final List<ChannelID> localReceivers = receiverList.getLocalReceivers();
			if (localReceivers.size() == 1) {
				// Unicast case, get final buffer provider

				final ChannelID localReceiver = localReceivers.get(0);
				final ChannelContext cc = this.registeredChannels.get(localReceiver);
				if (cc == null) {
					throw new IOException("Cannot find channel context for local receiver " + localReceiver);
				}

				if (!cc.isInputChannel()) {
					throw new IOException("Channel context for local receiver " + localReceiver
						+ " is not an input channel context");
				}

				final InputChannelContext icc = (InputChannelContext) cc;

				return icc;
			}
		}

		return this.transitBufferPool;
	}

	private void redistributeGlobalBuffers() {

		final int numberOfChannelsForMulticast = 10; // TODO: Make this configurable

		final int totalNumberOfBuffers = GlobalBufferPool.getInstance().getTotalNumberOfBuffers();
		int totalNumberOfChannels = this.registeredChannels.size();
		if (this.multicastEnabled) {
			totalNumberOfChannels += numberOfChannelsForMulticast;
		}
		final double buffersPerChannel = (double) totalNumberOfBuffers / (double) totalNumberOfChannels;
		if (buffersPerChannel < 1.0) {
			LOG.warn("System is low on memory buffers. This may result in reduced performance.");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Total number of buffers is " + totalNumberOfBuffers);
			LOG.debug("Total number of channels is " + totalNumberOfChannels);
		}

		if (this.localBufferPoolOwner.isEmpty()) {
			return;
		}

		final Iterator<LocalBufferPoolOwner> it = this.localBufferPoolOwner.values().iterator();
		while (it.hasNext()) {
			final LocalBufferPoolOwner lbpo = it.next();
			lbpo.setDesignatedNumberOfBuffers((int) Math.ceil(buffersPerChannel * lbpo.getNumberOfChannels()));
		}

		if (this.multicastEnabled) {
			this.transitBufferPool.setDesignatedNumberOfBuffers((int) Math.ceil(buffersPerChannel
				* numberOfChannelsForMulticast));
		}
	}

	public void reportCheckpointDecisions(final List<CheckpointDecision> checkpointDecisions) {

		for (final CheckpointDecision cd : checkpointDecisions) {

			final TaskContext taskContext = this.tasksWithUndecidedCheckpoints.remove(cd.getVertexID());

			if (taskContext == null) {
				LOG.error("Cannot report checkpoint decision for vertex " + cd.getVertexID());
				continue;
			}

			taskContext.setCheckpointDecisionAsynchronously(cd.getCheckpointDecision());
			taskContext.reportAsynchronousEvent();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean registerSpillingQueueWithNetworkConnection(final JobID jobID, final ChannelID sourceChannelID,
			final SpillingQueue spillingQueue) throws IOException, InterruptedException {

		final TransferEnvelopeReceiverList receiverList = getReceiverList(jobID, sourceChannelID);

		if (!receiverList.hasRemoteReceivers()) {
			return false;
		}

		final List<InetSocketAddress> remoteReceivers = receiverList.getRemoteReceivers();
		if (remoteReceivers.size() > 1) {
			LOG.error("Cannot register spilling queue for more than one remote receiver");
			return false;
		}

		this.networkConnectionManager.registerSpillingQueueWithNetworkConnection(remoteReceivers.get(0), spillingQueue);

		return true;
	}
}
