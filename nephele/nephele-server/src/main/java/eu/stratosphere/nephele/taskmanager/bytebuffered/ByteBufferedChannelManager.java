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
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProviderBroker;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTaskContext;
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

	private final Map<ExecutionVertexID, RuntimeTaskContext> tasksWithUndecidedCheckpoints = new ConcurrentHashMap<ExecutionVertexID, RuntimeTaskContext>();

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
		this.transitBufferPool = new LocalBufferPool("Transit buffer pool", 128, true);

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

		final Environment environment = task.getEnvironment();

		final TaskContext taskContext = task.createTaskContext(this, this.tasksWithUndecidedCheckpoints);

		final Set<GateID> outputGateIDs = environment.getOutputGateIDs();
		for (final Iterator<GateID> gateIt = outputGateIDs.iterator(); gateIt.hasNext();) {

			final GateID gateID = gateIt.next();
			final OutputGateContext outputGateContext = taskContext.createOutputGateContext(gateID);
			final Set<ChannelID> outputChannelIDs = environment.getOutputChannelIDsOfGate(gateID);
			for (final Iterator<ChannelID> channelIt = outputChannelIDs.iterator(); channelIt.hasNext();) {

				final ChannelID channelID = channelIt.next();
				final OutputChannelContext previousContext = (OutputChannelContext) this.registeredChannels
					.get(channelID);

				final boolean isActive = activeOutputChannels.contains(channelID);

				final OutputChannelContext outputChannelContext = outputGateContext.createOutputChannelContext(
					channelID, previousContext, isActive, this.mergeSpilledBuffers);

				// Add routing entry to receiver cache to reduce latency
				if (outputChannelContext.getType() == ChannelType.INMEMORY) {
					addReceiverListHint(outputChannelContext);
				}

				if (LOG.isDebugEnabled())
					LOG.debug("Registering byte buffered output channel " + outputChannelContext.getChannelID() + " ("
							+ (isActive ? "active" : "inactive") + ")");

				this.registeredChannels.put(outputChannelContext.getChannelID(), outputChannelContext);
			}
		}

		final Set<GateID> inputGateIDs = environment.getInputGateIDs();
		for (final Iterator<GateID> gateIt = inputGateIDs.iterator(); gateIt.hasNext();) {

			final GateID gateID = gateIt.next();
			final InputGateContext inputGateContext = taskContext.createInputGateContext(gateID);
			final Set<ChannelID> inputChannelIDs = environment.getInputChannelIDsOfGate(gateID);
			for (final Iterator<ChannelID> channelIt = inputChannelIDs.iterator(); channelIt.hasNext();) {

				final ChannelID channelID = channelIt.next();
				final InputChannelContext previousContext = (InputChannelContext) this.registeredChannels
					.get(channelID);

				final InputChannelContext inputChannelContext = inputGateContext.createInputChannelContext(
					channelID, previousContext);

				// Add routing entry to receiver cache to reduce latency
				if (inputChannelContext.getType() == ChannelType.INMEMORY) {
					addReceiverListHint(inputChannelContext);
				}

				final boolean isActive = activeOutputChannels.contains(inputChannelContext.getChannelID());

				if (LOG.isDebugEnabled())
					LOG.debug("Registering byte buffered input channel " + inputChannelContext.getChannelID() + " ("
							+ (isActive ? "active" : "inactive") + ")");

				this.registeredChannels.put(inputChannelContext.getChannelID(), inputChannelContext);
			}

			// Add input gate context to set of local buffer pool owner
			final LocalBufferPoolOwner bufferPoolOwner = inputGateContext.getLocalBufferPoolOwner();
			if (bufferPoolOwner != null) {
				this.localBufferPoolOwner.put(inputGateContext.getGateID(), bufferPoolOwner);
			}

		}

		final LocalBufferPoolOwner bufferPoolOwner = taskContext.getLocalBufferPoolOwner();
		if (bufferPoolOwner != null) {
			this.localBufferPoolOwner.put(task.getVertexID(), bufferPoolOwner);
		}

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

		final Environment environment = task.getEnvironment();

		Iterator<ChannelID> channelIterator = environment.getOutputChannelIDs().iterator();

		while (channelIterator.hasNext()) {

			final ChannelID outputChannelID = channelIterator.next();
			final ChannelContext context = this.registeredChannels.remove(outputChannelID);
			if (context != null) {
				context.destroy();
			}
			this.receiverCache.remove(outputChannelID);
		}

		channelIterator = environment.getInputChannelIDs().iterator();

		while (channelIterator.hasNext()) {

			final ChannelID outputChannelID = channelIterator.next();
			final ChannelContext context = this.registeredChannels.remove(outputChannelID);
			if (context != null) {
				context.destroy();
			}
			this.receiverCache.remove(outputChannelID);
		}

		final Iterator<GateID> inputGateIterator = environment.getInputGateIDs().iterator();

		while (inputGateIterator.hasNext()) {

			final GateID inputGateID = inputGateIterator.next();

			final LocalBufferPoolOwner owner = this.localBufferPoolOwner.remove(inputGateID);
			if (owner != null) {
				owner.clearLocalBufferPool();
			}
		}

		final LocalBufferPoolOwner owner = this.localBufferPoolOwner.remove(vertexID);
		if (owner != null) {
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

	public NetworkConnectionManager getNetworkConnectionManager() {

		return this.networkConnectionManager;
	}

	private void recycleBuffer(final TransferEnvelope envelope) {

		final Buffer buffer = envelope.getBuffer();
		if (buffer != null) {
			buffer.recycleBuffer();
		}
	}

	private void sendReceiverNotFoundEvent(final TransferEnvelope envelope, final ChannelID receiver) {

		if (envelope.getBuffer() == null && envelope.getSequenceNumber() == 0) {

			final EventList eventList = envelope.getEventList();
			if (eventList.size() == 1) {
				final AbstractEvent event = eventList.get(0);
				if (event instanceof ReceiverNotFoundEvent) {
					LOG.info("Dropping request to send ReceiverNotFoundEvent as response to ReceiverNotFoundEvent");
					return;
				}
			}
		}

		final JobID jobID = envelope.getJobID();

		final TransferEnvelope transferEnvelope = new TransferEnvelope(0, jobID, receiver);

		final ReceiverNotFoundEvent unknownReceiverEvent = new ReceiverNotFoundEvent(receiver,
			envelope.getSequenceNumber());
		transferEnvelope.addEvent(unknownReceiverEvent);

		final TransferEnvelopeReceiverList receiverList = getReceiverList(jobID, receiver);
		if (receiverList == null) {
			LOG.error("Cannot determine receiver list for source channel ID " + receiver);
			return;
		}

		processEnvelopeEnvelopeWithoutBuffer(transferEnvelope, receiverList);
	}

	private void processEnvelope(final TransferEnvelope transferEnvelope, final boolean freeSourceBuffer) {

		final TransferEnvelopeReceiverList receiverList = getReceiverList(transferEnvelope.getJobID(),
			transferEnvelope.getSource());

		if (receiverList == null) {
			recycleBuffer(transferEnvelope);
			return;
		}

		// This envelope is known to have either no buffer or an memory-based input buffer
		if (transferEnvelope.getBuffer() == null) {
			processEnvelopeEnvelopeWithoutBuffer(transferEnvelope, receiverList);
		} else {
			processEnvelopeWithBuffer(transferEnvelope, receiverList, freeSourceBuffer);
		}
	}

	private void processEnvelopeWithBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList, final boolean freeSourceBuffer) {

		// Handle the most common (unicast) case first
		if (!freeSourceBuffer) {

			final List<ChannelID> localReceivers = receiverList.getLocalReceivers();
			if (localReceivers.size() != 1) {
				LOG.error("Expected receiver list to have exactly one element");
			}

			final ChannelID localReceiver = localReceivers.get(0);

			final ChannelContext cc = this.registeredChannels.get(localReceiver);
			if (cc == null) {

				sendReceiverNotFoundEvent(transferEnvelope, localReceiver);
				recycleBuffer(transferEnvelope);
				return;
			}

			if (!cc.isInputChannel()) {
				LOG.error("Local receiver " + localReceiver
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

					sendReceiverNotFoundEvent(transferEnvelope, localReceiver);
					continue;
				}

				if (!cc.isInputChannel()) {
					LOG.error("Local receiver " + localReceiver
						+ " is not an input channel, but is supposed to accept a buffer");
					continue;
				}

				final InputChannelContext inputChannelContext = (InputChannelContext) cc;

				Buffer destBuffer = null;
				try {
					destBuffer = inputChannelContext.requestEmptyBufferBlocking(srcBuffer.size());
					srcBuffer.copyToBuffer(destBuffer);
				} catch (Exception e) {
					LOG.error(StringUtils.stringifyException(e));
					if (destBuffer != null) {
						destBuffer.recycleBuffer();
					}
					continue;
				}
				// TODO: See if we can save one duplicate step here
				final TransferEnvelope dup = transferEnvelope.duplicateWithoutBuffer();
				dup.setBuffer(destBuffer);
				inputChannelContext.queueTransferEnvelope(dup);
			}
		}

		if (receiverList.hasRemoteReceivers()) {

			final List<InetSocketAddress> remoteReceivers = receiverList.getRemoteReceivers();
			for (final InetSocketAddress remoteReceiver : remoteReceivers) {

				TransferEnvelope dup = null;
				try {
					dup = transferEnvelope.duplicate();
				} catch (Exception e) {
					LOG.error(StringUtils.stringifyException(e));
					if (dup != null) {
						recycleBuffer(dup);
						continue;
					}
				}

				this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, dup);
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
				sendReceiverNotFoundEvent(transferEnvelope, localReceiver);
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

	private void addReceiverListHint(final ChannelContext channelContext) {

		TransferEnvelopeReceiverList receiverList = new TransferEnvelopeReceiverList(channelContext);

		if (this.receiverCache.put(channelContext.getChannelID(), receiverList) != null) {
			LOG.warn("Receiver cache already contained entry for " + channelContext.getChannelID());
		}
	}

	private TransferEnvelopeReceiverList getReceiverList(final JobID jobID, final ChannelID sourceChannelID) {

		TransferEnvelopeReceiverList receiverList = this.receiverCache.get(sourceChannelID);

		if (receiverList == null) {

			try {
				while (true) {

					ConnectionInfoLookupResponse lookupResponse;
					synchronized (this.channelLookupService) {
						lookupResponse = this.channelLookupService.lookupConnectionInfo(
							this.localConnectionInfo, jobID, sourceChannelID);
					}

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

				if (receiverList != null) {

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
			} catch (InterruptedException ie) {
				// TODO: Send appropriate notifications here
			} catch (IOException ioe) {
				// TODO: Send appropriate notifications here
			}
		}

		return receiverList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) {

		processEnvelope(transferEnvelope, true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) {

		processEnvelope(transferEnvelope, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromNetwork(final TransferEnvelope transferEnvelope, boolean freeSourceBuffer) {

		processEnvelope(transferEnvelope, freeSourceBuffer);
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

	/**
	 * {@inheritDoc}
	 */
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

					// Use the transit buffer for this purpose, data will be discarded in most cases anyway.
					return this.transitBufferPool;
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

			final RuntimeTaskContext taskContext = this.tasksWithUndecidedCheckpoints.remove(cd.getVertexID());

			if (taskContext == null) {
				LOG.error("Cannot report checkpoint decision for vertex " + cd.getVertexID());
				continue;
			}
			LOG.info("reporting checkpoint decision for " + cd.getVertexID());
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

	/**
	 * Invalidates the entries identified by the given channel IDs from the receiver lookup cache.
	 * 
	 * @param channelIDs
	 *        the channel IDs identifying the cache entries to invalidate
	 */
	public void invalidateLookupCacheEntries(final Set<ChannelID> channelIDs) {

		final Iterator<ChannelID> it = channelIDs.iterator();
		while (it.hasNext()) {

			this.receiverCache.remove(it.next());
		}
	}
}
