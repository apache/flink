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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProviderBroker;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeReceiverList;
import eu.stratosphere.nephele.util.StringUtils;

public final class ByteBufferedChannelManager implements TransferEnvelopeDispatcher, BufferProviderBroker {

	/**
	 * The log object used to report problems and errors.
	 */
	private static final Log LOG = LogFactory.getLog(ByteBufferedChannelManager.class);

	private final Map<ChannelID, ChannelContext> registeredChannels = new HashMap<ChannelID, ChannelContext>();

	private final NetworkConnectionManager networkConnectionManager;

	private final ChannelLookupProtocol channelLookupService;

	private final InstanceConnectionInfo localConnectionInfo;

	private final CanceledChannelSet canceledChannelSet;

	private final FileBufferManager fileBufferManager;

	private final Map<ExecutionVertexID, TaskContext> taskMap = new HashMap<ExecutionVertexID, TaskContext>();

	/**
	 * This map caches transfer envelope receiver lists.
	 */
	private final Map<ChannelID, TransferEnvelopeReceiverList> receiverCache = new HashMap<ChannelID, TransferEnvelopeReceiverList>();

	public ByteBufferedChannelManager(ChannelLookupProtocol channelLookupService,
			InstanceConnectionInfo localInstanceConnectionInfo)
												throws IOException {

		this.channelLookupService = channelLookupService;

		this.localConnectionInfo = localInstanceConnectionInfo;

		this.canceledChannelSet = new CanceledChannelSet();

		this.fileBufferManager = new FileBufferManager(this.canceledChannelSet);

		// Initialize the global buffer pool
		GlobalBufferPool.getInstance();

		this.networkConnectionManager = new NetworkConnectionManager(this,
			localInstanceConnectionInfo.getAddress(),
			localInstanceConnectionInfo.getDataPort());
	}

	/**
	 * Registers the given task with the byte buffered channel manager.
	 * 
	 * @param vertexID
	 *        the ID of the task to be registered
	 * @param environment
	 *        the environment of the task
	 */
	public void register(final ExecutionVertexID vertexID, final Environment environment) {

		final TaskContext taskContext = new TaskContext();

		synchronized (this.registeredChannels) {

			for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
				final OutputGate<?> outputGate = environment.getOutputGate(i);
				final OutputGateContext outputGateContext = new OutputGateContext(taskContext, outputGate, this,
					this.fileBufferManager);
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

					LOG.info("Registering byte buffered input channel " + bboc.getID());

					final OutputChannelContext outputChannelContext = new OutputChannelContext(outputGateContext, bboc);
					this.registeredChannels.put(bboc.getID(), outputChannelContext);
				}
			}

			for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
				final InputGate<?> inputGate = environment.getInputGate(i);
				final InputGateContext inputGateContext = new InputGateContext(taskContext);
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

					LOG.info("Registering byte buffered input channel " + bbic.getID());

					final InputChannelContext inputChannelContext = new InputChannelContext(inputGateContext, this,
						bbic);
					this.registeredChannels.put(bbic.getID(), inputChannelContext);
				}
			}
		}

		synchronized (this.taskMap) {
			this.taskMap.put(vertexID, taskContext);
		}
		
		redistributeGlobalBuffers();
	}

	/**
	 * Unregisters the given task from the byte buffered channel manager.
	 * 
	 * @param vertexID
	 *        the ID of the task to be unregistered
	 * @param environment
	 *        the environment of the task
	 */
	public void unregister(final ExecutionVertexID vertexID, final Environment environment) {

		synchronized (this.registeredChannels) {

			for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
				final OutputGate<?> outputGate = environment.getOutputGate(i);
				for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
					final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(j);
					this.registeredChannels.remove(outputChannel.getID());
				}
			}

			for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
				final InputGate<?> inputGate = environment.getInputGate(i);
				for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {
					final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(j);
					this.registeredChannels.remove(inputChannel.getID());
				}
			}
		}
		
		synchronized (this.taskMap) {
			final TaskContext taskContext = this.taskMap.remove(vertexID);
			if(taskContext == null) {
				LOG.error("taskContext is null!");
				return;
			}
			
			taskContext.releaseAllResources();
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

		synchronized (this.registeredChannels) {

			final Iterator<ChannelContext> it = this.registeredChannels.values().iterator();

			while (it.hasNext()) {

				final ChannelContext channelContext = it.next();
				if (channelContext.isInputChannel()) {
					channelContext.reportIOException(ioe);
				}
			}
		}
	}

	public void reportIOExceptionForOutputChannel(ChannelID sourceChannelID, IOException ioe) {

		ChannelContext channelContext = null;
		synchronized (this.registeredChannels) {
			channelContext = this.registeredChannels.get(sourceChannelID);
		}
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

	private boolean processEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		final TransferEnvelopeReceiverList receiverList = getReceiverList(transferEnvelope.getJobID(),
			transferEnvelope.getSource());

		if (receiverList == null) {
			throw new IOException("Transfer envelope " + transferEnvelope.getSequenceNumber() + " from source channel "
				+ transferEnvelope.getSource() + " has not have a receiver list");
		}

		if (receiverList.getTotalNumberOfReceivers() == 0) {
			throw new IOException("Total number of receivers for envelope " + transferEnvelope.getSequenceNumber()
				+ " from source channel " + transferEnvelope.getSource() + " is 0");
		}

		// This envelope is known to have either no buffer or an memory-based input buffer
		if (transferEnvelope.getBuffer() == null) {
			return processEnvelopeEnvelopeWithoutBuffer(transferEnvelope, receiverList);
		} else {
			return processEnvelopeWithBuffer(transferEnvelope, receiverList, null);
		}
	}

	private boolean processEnvelopeWithBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList, final BufferProvider bufferProvider) throws IOException,
			InterruptedException {

		System.out.println("Processing envelope " + transferEnvelope.getSequenceNumber());

		// Throw everything away
		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			buffer.recycleBuffer();
		}

		return true;
	}

	private boolean processEnvelopeEnvelopeWithoutBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList) {

		System.out.println("Received envelope without buffer with event list size "
			+ transferEnvelope.getEventList().size());

		// No need to copy anything
		final Iterator<ChannelID> localIt = receiverList.getLocalReceivers().iterator();

		while (localIt.hasNext()) {

			final ChannelID localReceiver = localIt.next();
			synchronized (this.registeredChannels) {

				final ChannelContext channelContext = this.registeredChannels.get(localReceiver);
				if (channelContext == null) {
					LOG.error("Cannot find local receiver " + localReceiver + " for job "
						+ transferEnvelope.getJobID());
					continue;
				}
				channelContext.queueTransferEnvelope(transferEnvelope);
			}
		}

		final Iterator<InetSocketAddress> remoteIt = receiverList.getRemoteReceivers().iterator();

		while (remoteIt.hasNext()) {

			final InetSocketAddress remoteReceiver = remoteIt.next();
			this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, transferEnvelope);
		}

		return true;
	}

	private TransferEnvelopeReceiverList getReceiverList(final JobID jobID, final ChannelID sourceChannelID)
			throws IOException, InterruptedException {

		synchronized (this.receiverCache) {

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

					System.out.println("Receiver list for source channel ID " + sourceChannelID + " at task manager "
						+ this.localConnectionInfo);
					if (receiverList.hasLocalReceivers()) {
						System.out.println("\tLocal receivers:");
						final Iterator<ChannelID> it = receiverList.getLocalReceivers().iterator();
						while (it.hasNext()) {
							System.out.println("\t\t" + it.next());
						}
					}

					if (receiverList.hasRemoteReceivers()) {
						System.out.println("Remote receivers:");
						final Iterator<InetSocketAddress> it = receiverList.getRemoteReceivers().iterator();
						while (it.hasNext()) {
							System.out.println("\t\t" + it.next());
						}
					}
				}
			}

			return receiverList;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		System.out.println("Received envelope from input channel");

		processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processEnvelopeFromNetworkOrCheckpoint(final TransferEnvelope transferEnvelope) throws IOException {

		// System.out.println("Processing envelope "+ transferEnvelope.getSequenceNumber() + " from network");

		try {
			if (!processEnvelope(transferEnvelope)) {
				// System.out.println("Processing envelope from network returned false");
				return false;
			}
		} catch (InterruptedException e) {
			LOG.error("Caught unexpected interrupted exception: " + StringUtils.stringifyException(e));
		}

		return true;
	}

	/**
	 * Triggers the byte buffer channel manager write the current utilization of its read and write buffers to the logs.
	 * This method is primarily for debugging purposes.
	 */
	public void logBufferUtilization() {

		System.out.println("Buffer utilization for at " + System.currentTimeMillis());
		// TODO: Fix me
		/*
		 * synchronized (this.emptyWriteBuffers) {
		 * System.out.println("\tEmpty write buffers: " + this.emptyWriteBuffers.size());
		 * }
		 * synchronized (this.emptyReadBuffers) {
		 * System.out.println("\tEmpty read buffers: " + this.emptyReadBuffers.size());
		 * }
		 */

		this.networkConnectionManager.logBufferUtilization();

		synchronized (this.registeredChannels) {

			System.out.println("\tIncoming connections:");

			final Iterator<Map.Entry<ChannelID, ChannelContext>> it = this.registeredChannels.entrySet()
				.iterator();

			while (it.hasNext()) {

				final Map.Entry<ChannelID, ChannelContext> entry = it.next();
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
	}

	/**
	 * Triggers the clean-up method of the canceled channel set.
	 */
	public void cleanUpCanceledChannelSet() {

		this.canceledChannelSet.cleanup();
	}

	/**
	 * Marks the channel with the given ID as canceled. This means the channel belongs to a task which as about to be
	 * canceled.
	 * 
	 * @param channelID
	 *        the ID of the channel to be marked as canceled
	 */
	public void markChannelAsCanceled(ChannelID channelID) {

		this.canceledChannelSet.add(channelID);
	}

	/**
	 * Returns the instance of the file buffer manager that is used by this byte buffered channel manager.
	 * 
	 * @return the instance of the file buffer manager that is used by this byte buffered channel manager.
	 */
	public FileBufferManager getFileBufferManager() {
		return this.fileBufferManager;
	}

	@Override
	public BufferProvider getBufferProvider(ChannelID channelID) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void redistributeGlobalBuffers() {
		
		final int totalNumberOfBuffers = GlobalBufferPool.getInstance().getTotalNumberOfBuffers();
		
		synchronized(this.taskMap) {
			
			final int buffersPerTask = (int) Math.ceil((double) totalNumberOfBuffers / (double) this.taskMap.size());
			final Iterator<TaskContext> it = this.taskMap.values().iterator();
			while(it.hasNext()) {
				it.next().setBufferLimit(buffersPerTask);
			}
		}
		
	}
}
