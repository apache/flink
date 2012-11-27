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

package eu.stratosphere.nephele.taskmanager.routing;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
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
import eu.stratosphere.nephele.taskmanager.network.NetworkService;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class DefaultRoutingService implements RoutingService, BufferProviderBroker {

	/**
	 * The log object used to report problems and errors.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultRoutingService.class);

	private static final boolean DEFAULT_ALLOW_SENDER_SIDE_SPILLING = false;

	private static final boolean DEFAULT_MERGE_SPILLED_BUFFERS = true;

	// TODO: Make this configurable
	private static final int NUMBER_OF_CHANNELS_FOR_MULTICAST = 10;

	private final Map<ChannelID, ChannelContext> registeredChannels = new ConcurrentHashMap<ChannelID, ChannelContext>();

	private final Map<AbstractID, LocalBufferPoolOwner> localBufferPoolOwner = new ConcurrentHashMap<AbstractID, LocalBufferPoolOwner>();

	private final NetworkService networkService;

	private final ChannelLookupProtocol channelLookupService;

	private final InstanceConnectionInfo localConnectionInfo;

	private final LocalBufferPool transitBufferPool;

	private final boolean allowSenderSideSpilling;

	private final boolean mergeSpilledBuffers;

	private final boolean multicastEnabled = true;

	/**
	 * This map caches transfer envelope receiver lists.
	 */
	private final Map<ChannelID, ReceiverList> receiverCache = new ConcurrentHashMap<ChannelID, ReceiverList>();

	public DefaultRoutingService(final ChannelLookupProtocol channelLookupService,
			final InstanceConnectionInfo localInstanceConnectionInfo) throws IOException {

		this.channelLookupService = channelLookupService;

		this.localConnectionInfo = localInstanceConnectionInfo;

		// Initialized the file buffer manager
		FileBufferManager.getInstance();

		// Initialize the global buffer pool
		GlobalBufferPool.getInstance();

		// Initialize the transit buffer pool
		this.transitBufferPool = new LocalBufferPool(128, true);

		this.networkService = new NetworkService(this, localInstanceConnectionInfo.getAddress(),
			localInstanceConnectionInfo.getDataPort());

		this.allowSenderSideSpilling = GlobalConfiguration.getBoolean("channel.network.allowSenderSideSpilling",
			DEFAULT_ALLOW_SENDER_SIDE_SPILLING);

		this.mergeSpilledBuffers = GlobalConfiguration.getBoolean("channel.network.mergeSpilledBuffers",
			DEFAULT_MERGE_SPILLED_BUFFERS);

		LOG.info("Initialized default routing service with sender-side spilling "
			+ (this.allowSenderSideSpilling ? "enabled" : "disabled")
			+ (this.mergeSpilledBuffers ? " and spilled buffer merging enabled" : ""));
	}

	/**
	 * Registers the given task with the routing service.
	 * 
	 * @param task
	 *        the task to be registered
	 * @param activeOutputChannels
	 *        the set of output channels which are initially active
	 * @param hasAlreadyBeenDeployed
	 *        stores if the task has already been deployed before at least once
	 * @throws InsufficientResourcesException
	 *         thrown if the channel manager does not have enough memory buffers to safely run this task
	 */
	public void register(final Task task, final Set<ChannelID> activeOutputChannels,
			final boolean hasAlreadyBeenDeployed) throws InsufficientResourcesException {

		// Check if we can safely run this task with the given resources
		checkBufferAvailability(task);

		final Environment environment = task.getEnvironment();

		final TaskContext taskContext = task.createTaskContext(this,
			this.localBufferPoolOwner.remove(task.getVertexID()));

		final Set<GateID> outputGateIDs = environment.getOutputGateIDs();
		for (final Iterator<GateID> gateIt = outputGateIDs.iterator(); gateIt.hasNext();) {

			final GateID gateID = gateIt.next();
			final OutputGateContext outputGateContext = taskContext.createOutputGateContext(gateID);
			final Set<ChannelID> outputChannelIDs = environment.getOutputChannelIDsOfGate(gateID);
			for (final Iterator<ChannelID> channelIt = outputChannelIDs.iterator(); channelIt.hasNext();) {

				final ChannelID channelID = channelIt.next();
				final OutputChannelContext previousContext = (OutputChannelContext) this.registeredChannels
					.get(channelID);

				final boolean isActive = true;/* activeOutputChannels.contains(channelID); */

				final OutputChannelContext outputChannelContext = outputGateContext.createOutputChannelContext(
					channelID, previousContext, isActive, this.mergeSpilledBuffers);

				// Add routing entry to receiver cache to reduce latency if this is the task's first execution
				if (outputChannelContext.getType() == ChannelType.INMEMORY && !hasAlreadyBeenDeployed) {
					addReceiverListHint(outputChannelContext);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Registering output channel " + outputChannelContext.getChannelID() + " ("
						+ (isActive ? "active" : "inactive") + ")");
				}

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

				// Add routing entry to receiver cache to reduce latency if this is the task's first execution
				if (inputChannelContext.getType() == ChannelType.INMEMORY && !hasAlreadyBeenDeployed) {
					addReceiverListHint(inputChannelContext);
				}

				this.registeredChannels.put(inputChannelContext.getChannelID(), inputChannelContext);
			}

			// Add input gate context to set of local buffer pool owner
			final LocalBufferPoolOwner bufferPoolOwner = inputGateContext.getLocalBufferPoolOwner();
			if (bufferPoolOwner != null) {
				this.localBufferPoolOwner.put(inputGateContext.getGateID(), bufferPoolOwner);
			}

		}

		this.localBufferPoolOwner.put(task.getVertexID(), taskContext);

		redistributeGlobalBuffers();
	}

	/**
	 * Unregisters the given task from the routing service.
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
	 * Shuts down the routing service and stops all its internal services.
	 */
	public void shutdown() {

		this.networkService.shutDown();
	}

	private void recycleBuffer(final TransferEnvelope envelope) {

		final Buffer buffer = envelope.getBuffer();
		if (buffer != null) {
			buffer.recycleBuffer();
		}
	}

	private void sendReceiverNotFoundEvent(final TransferEnvelope envelope, final ChannelID receiver)
			throws IOException, InterruptedException {

		if (ReceiverNotFoundEvent.isReceiverNotFoundEvent(envelope)) {

			LOG.info("Dropping request to send ReceiverNotFoundEvent as response to ReceiverNotFoundEvent");
			return;
		}

		final JobID jobID = envelope.getJobID();

		final TransferEnvelope transferEnvelope = ReceiverNotFoundEvent.createEnvelopeWithEvent(jobID, receiver,
			envelope.getSequenceNumber());

		final ReceiverList receiverList = getReceiverList(jobID, receiver);
		if (receiverList == null) {
			return;
		}

		processEnvelopeEnvelopeWithoutBuffer(transferEnvelope, receiverList);
	}

	private void processEnvelope(final TransferEnvelope transferEnvelope, final boolean freeSourceBuffer)
			throws IOException, InterruptedException {

		ReceiverList receiverList = null;
		try {
			receiverList = getReceiverList(transferEnvelope.getJobID(),
				transferEnvelope.getSource());
		} catch (InterruptedException e) {
			recycleBuffer(transferEnvelope);
			throw e;
		} catch (IOException e) {
			recycleBuffer(transferEnvelope);
			throw e;
		}

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
			final ReceiverList receiverList, final boolean freeSourceBuffer)
			throws IOException, InterruptedException {

		// Handle the most common (unicast) case first
		if (!freeSourceBuffer) {

			final List<ChannelID> localReceivers = receiverList.getLocalReceivers();
			if (localReceivers.size() != 1) {
				LOG.error("Expected receiver list to have exactly one element");
			}

			final ChannelID localReceiver = localReceivers.get(0);

			final ChannelContext cc = this.registeredChannels.get(localReceiver);
			if (cc == null) {

				try {
					sendReceiverNotFoundEvent(transferEnvelope, localReceiver);
				} finally {
					recycleBuffer(transferEnvelope);
				}
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

		try {

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

					final Buffer destBuffer = inputChannelContext.requestEmptyBufferBlocking(srcBuffer.size());
					try {
						srcBuffer.copyToBuffer(destBuffer);
					} catch (IOException e) {
						destBuffer.recycleBuffer();
						throw e;
					}
					// TODO: See if we can save one duplicate step here
					final TransferEnvelope dup = transferEnvelope.duplicateWithoutBuffer();
					dup.setBuffer(destBuffer);
					inputChannelContext.queueTransferEnvelope(dup);
				}
			}

			if (receiverList.hasRemoteReceivers()) {

				final List<RemoteReceiver> remoteReceivers = receiverList.getRemoteReceivers();

				for (final RemoteReceiver remoteReceiver : remoteReceivers) {

					final TransferEnvelope dup = transferEnvelope.duplicate();
					this.networkService.queueEnvelopeForTransfer(remoteReceiver, dup);
				}
			}
		} finally {
			// Recycle the source buffer
			srcBuffer.recycleBuffer();
		}
	}

	private void processEnvelopeEnvelopeWithoutBuffer(final TransferEnvelope transferEnvelope,
			final ReceiverList receiverList) throws IOException, InterruptedException {

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

		if (!receiverList.hasRemoteReceivers()) {
			return;
		}

		// Generate sender hint before sending the first envelope over the network
		final List<RemoteReceiver> remoteReceivers = receiverList.getRemoteReceivers();
		final Iterator<RemoteReceiver> remoteIt = remoteReceivers.iterator();

		while (remoteIt.hasNext()) {

			final RemoteReceiver remoteReceiver = remoteIt.next();
			this.networkService.queueEnvelopeForTransfer(remoteReceiver, transferEnvelope);
		}
	}

	private void addReceiverListHint(final ChannelContext channelContext) {

		final ReceiverList receiverList = new ReceiverList(channelContext);

		if (this.receiverCache.put(channelContext.getChannelID(), receiverList) != null) {
			LOG.warn("Receiver cache already contained entry for " + channelContext.getChannelID());
		}
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
	 * @throws InterruptedExcption
	 */
	private ReceiverList getReceiverList(final JobID jobID, final ChannelID sourceChannelID)
			throws IOException, InterruptedException {

		ReceiverList receiverList = this.receiverCache.get(sourceChannelID);

		if (receiverList != null) {
			return receiverList;
		}

		while (true) {

			if (Thread.currentThread().isInterrupted()) {
				break;
			}

			final ConnectionInfoLookupResponse lookupResponse = this.channelLookupService.lookupConnectionInfo(
				this.localConnectionInfo, jobID, sourceChannelID);

			if (lookupResponse.isJobAborting()) {
				break;
			}

			if (lookupResponse.receiverNotFound()) {
				LOG.error("Cannot find task(s) waiting for data from source channel with ID " + sourceChannelID);
				break;
			}

			if (lookupResponse.receiverNotReady()) {
				Thread.sleep(500);
				continue;
			}

			if (lookupResponse.receiverReady()) {
				receiverList = new ReceiverList(lookupResponse);
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
					final Iterator<RemoteReceiver> it = receiverList.getRemoteReceivers().iterator();
					while (it.hasNext()) {
						sb.append("\t\t" + it.next() + "\n");
					}
				}

				LOG.debug(sb.toString());
			}
		}

		return receiverList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void routeEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope, true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void routeEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void routeEnvelopeFromNetwork(final TransferEnvelope transferEnvelope, boolean freeSourceBuffer)
			throws IOException, InterruptedException {

		processEnvelope(transferEnvelope, freeSourceBuffer);
	}

	/**
	 * Triggers the routing service write the current utilization of its read and write buffers to the logs.
	 * This method is primarily for debugging purposes.
	 */
	public void logBufferUtilization() {

		System.out.println("Buffer utilization at " + System.currentTimeMillis());

		System.out.println("\tUnused global buffers: " + GlobalBufferPool.getInstance().getCurrentNumberOfBuffers());

		System.out.println("\tLocal buffer pool status:");

		final Iterator<LocalBufferPoolOwner> it = this.localBufferPoolOwner.values().iterator();
		while (it.hasNext()) {
			it.next().logBufferUtilization();
		}

		this.networkService.logBufferUtilization();

		System.out.println("\tIncoming connections:");

		final Iterator<Map.Entry<ChannelID, ChannelContext>> it2 = this.registeredChannels.entrySet()
			.iterator();

		while (it2.hasNext()) {

			final Map.Entry<ChannelID, ChannelContext> entry = it2.next();
			final ChannelContext context = entry.getValue();
			if (context.isInputChannel()) {

				final InputChannelContext inputChannelContext = (InputChannelContext) context;
				inputChannelContext.logQueuedEnvelopes();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BufferProvider getBufferProvider(final JobID jobID, final ChannelID sourceChannelID) throws IOException,
			InterruptedException {

		final ReceiverList receiverList = getReceiverList(jobID, sourceChannelID);

		// Receiver could not be determined, use transit buffer pool to read data from channel
		if (receiverList == null) {
			return this.transitBufferPool;
		}

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

	/**
	 * Checks if the routing service has enough resources available to safely execute the given task.
	 * 
	 * @param task
	 *        the task to be executed
	 * @throws InsufficientResourcesException
	 *         thrown if the routing service currently does not have enough resources available to execute the task
	 */
	private void checkBufferAvailability(final Task task) throws InsufficientResourcesException {

		final int totalNumberOfBuffers = GlobalBufferPool.getInstance().getTotalNumberOfBuffers();
		int numberOfAlreadyRegisteredChannels = this.registeredChannels.size();
		if (this.multicastEnabled) {
			numberOfAlreadyRegisteredChannels += NUMBER_OF_CHANNELS_FOR_MULTICAST;
		}

		final Environment env = task.getEnvironment();

		final int numberOfNewChannels = env.getNumberOfOutputChannels() + env.getNumberOfInputChannels();
		final int totalNumberOfChannels = numberOfAlreadyRegisteredChannels + numberOfNewChannels;

		final double buffersPerChannel = (double) totalNumberOfBuffers
			/ (double) totalNumberOfChannels;

		if (buffersPerChannel < 1.0) {

			// Construct error message
			final StringBuilder sb = new StringBuilder(this.localConnectionInfo.getHostName());
			sb.append(" has not enough buffers available to safely execute ");
			sb.append(env.getTaskName());
			sb.append(" (");
			sb.append(totalNumberOfChannels - totalNumberOfBuffers);
			sb.append(" buffers are currently missing)");

			throw new InsufficientResourcesException(sb.toString());
		}
	}

	/**
	 * Redistributes the global buffers among the registered tasks.
	 */
	private void redistributeGlobalBuffers() {

		final int totalNumberOfBuffers = GlobalBufferPool.getInstance().getTotalNumberOfBuffers();
		int totalNumberOfChannels = this.registeredChannels.size();
		if (this.multicastEnabled) {
			totalNumberOfChannels += NUMBER_OF_CHANNELS_FOR_MULTICAST;
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
			lbpo.setDesignatedNumberOfBuffers((int) Math.ceil(buffersPerChannel
				* lbpo.getMinimumNumberOfRequiredBuffers()));
		}

		if (this.multicastEnabled) {
			this.transitBufferPool.setDesignatedNumberOfBuffers((int) Math.ceil(buffersPerChannel
				* NUMBER_OF_CHANNELS_FOR_MULTICAST));
		}
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

	public void reportAsynchronousEvent(final ExecutionVertexID vertexID) {

		final LocalBufferPoolOwner lbpo = this.localBufferPoolOwner.get(vertexID);
		if (lbpo == null) {
			System.out.println("Cannot find local buffer pool owner for " + vertexID);
			return;
		}

		lbpo.reportAsynchronousEvent();
	}
}
