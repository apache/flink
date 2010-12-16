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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;

public class OutgoingConnection {

	private static final Log LOG = LogFactory.getLog(OutgoingConnection.class);

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final InetSocketAddress connectionAddress;

	private final OutgoingConnectionThread connectionThread;

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	private final TransferEnvelopeSerializer serializer = new TransferEnvelopeSerializer();

	private TransferEnvelope currentEnvelope = null;

	private boolean isConnected = false;

	private final int numberOfConnectionRetries;

	private int retriesLeft = 0;

	private long timstampOfLastRetry = 0;

	private static long RETRYINTERVAL = 1000L; // 1 second

	public OutgoingConnection(ByteBufferedChannelManager byteBufferedChannelManager,
			InetSocketAddress connectionAddress, OutgoingConnectionThread connectionThread,
			int numberOfConnectionRetries) {

		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.connectionAddress = connectionAddress;
		this.connectionThread = connectionThread;
		this.numberOfConnectionRetries = numberOfConnectionRetries;
	}

	public void queueEnvelope(TransferEnvelope transferEnvelope) {

		synchronized (this.queuedEnvelopes) {

			if (!this.isConnected) {
				this.retriesLeft = numberOfConnectionRetries;
				this.timstampOfLastRetry = System.currentTimeMillis();
				this.connectionThread.triggerConnect(this);
				this.isConnected = true;
			}

			this.queuedEnvelopes.add(transferEnvelope);
		}
	}

	public InetSocketAddress getConnectionAddress() {
		return this.connectionAddress;
	}

	public void reportConnectionProblem(SocketChannel socketChannel, SelectionKey key, IOException ioe) {

		// First, write exception to log
		final long currentTime = System.currentTimeMillis();
		if (currentTime - this.timstampOfLastRetry >= RETRYINTERVAL) {
			LOG.error("Cannot connect to " + this.connectionAddress + ", " + this.retriesLeft + " retries left");
		}

		synchronized (this.queuedEnvelopes) {

			// Make sure the socket is closed
			if (socketChannel != null) {
				try {
					socketChannel.close();
				} catch (IOException e) {
					LOG.debug("Error while trying to close the socket channel to " + this.connectionAddress);
				}
			}

			// Cancel the key
			if (key != null) {
				key.cancel();
			}

			if (hasRetriesLeft(currentTime)) {
				this.connectionThread.triggerConnect(this);
				this.isConnected = true;
				return;
			}

			this.isConnected = false;

			// Error is fatal
			LOG.error(ioe);

			// Notify source of current envelope and release buffer
			if (this.currentEnvelope != null) {
				this.byteBufferedChannelManager
					.reportIOExceptionForOutputChannel(this.currentEnvelope.getSource(), ioe);
				if (this.currentEnvelope.getBuffer() != null) {
					this.currentEnvelope.getBuffer().recycleBuffer();
					this.currentEnvelope = null;
				}
			}

			// Notify all other tasks which are waiting for data to be transmitted
			final Iterator<TransferEnvelope> iter = this.queuedEnvelopes.iterator();
			while (iter.hasNext()) {
				final TransferEnvelope envelope = iter.next();
				this.byteBufferedChannelManager.reportIOExceptionForOutputChannel(envelope.getSource(), ioe);
				// Recycle the buffer inside the envelope
				if (envelope.getBuffer() != null) {
					envelope.getBuffer().recycleBuffer();
				}
			}

			this.queuedEnvelopes.clear();
		}
	}

	public void reportTransmissionProblem(SocketChannel socketChannel, SelectionKey key, IOException ioe) {

		// First, write exception to log
		if (this.currentEnvelope != null) {
			LOG.error("The connection between " + socketChannel.socket().getLocalAddress() + " and "
				+ socketChannel.socket().getRemoteSocketAddress()
				+ " experienced an IOException for transfer envelope " + this.currentEnvelope.getSequenceNumber());
		} else {
			LOG.error("The connection between " + socketChannel.socket().getLocalAddress() + " and "
				+ socketChannel.socket().getRemoteSocketAddress() + " experienced an IOException");
		}
		LOG.error(ioe);

		// Close the connection and cancel the interest key
		synchronized (this.queuedEnvelopes) {
			try {
				LOG.debug("Closing connection to " + socketChannel.socket().getRemoteSocketAddress());
				socketChannel.close();
			} catch (IOException e) {
				LOG.debug("An error occurred while responding to an IOException");
				LOG.debug(e);
			}

			key.cancel();

			// Trigger new connection, because we cannot rely on a newly queued enveloped to do so
			this.connectionThread.triggerConnect(this);
			this.timstampOfLastRetry = System.currentTimeMillis();
			this.isConnected = true;
		}

		// If we were transmitting an envelope,
		if (this.currentEnvelope != null) {
			// Increase transmission error counter //TODO: FIX ME
			/*
			 * this.currentEnvelope.increaseNumberOfTransmissionErrors();
			 * if(this.currentEnvelope.hasAnotherTransmissionAttempt()) {
			 * System.out.println("OUTGOING: Transmission error during " + this.currentEnvelope.getSequenceNumber());
			 * this.serializer.reset();
			 * if(this.currentEnvelope.getBuffer() != null) {
			 * //Retransmit the entire buffer
			 * //TODO: Fix me
			 * //this.currentEnvelope.getBuffer().position(0);
			 * }
			 * } else {
			 * //Too many transmission errors
			 * LOG.error("Fatal number of transmission errors for envelope from " + this.currentEnvelope.getSource());
			 * this.byteBufferedChannelManager.reportIOExceptionForOutputChannel(this.currentEnvelope.getSource(), ioe);
			 * if(this.currentEnvelope.getBuffer() != null) {
			 * this.currentEnvelope.getBuffer().recycleBuffer();
			 * }
			 * this.currentEnvelope = null;
			 * }
			 */
		} else {
			// TODO: Handle this case
		}
	}

	private boolean hasRetriesLeft(long currentTime) {

		if (currentTime - this.timstampOfLastRetry >= RETRYINTERVAL) {
			this.retriesLeft--;
			this.timstampOfLastRetry = currentTime;
			if (this.retriesLeft == 0) {
				return false;
			}
		}

		return true;
	}

	public boolean write(WritableByteChannel writableByteChannel) throws IOException {

		if (this.currentEnvelope == null) {
			synchronized (this.queuedEnvelopes) {
				if (this.queuedEnvelopes.isEmpty()) {
					return false;
				} else {
					this.currentEnvelope = this.queuedEnvelopes.peek();
					this.serializer.setTransferEnvelope(this.currentEnvelope);
				}
			}
		}

		if (!this.serializer.write(writableByteChannel)) {

			// Make sure we recycle the attached memory or file buffers correctly
			if (this.currentEnvelope.getBuffer() != null) {
				final TransferEnvelopeProcessingLog processingLog = this.currentEnvelope.getProcessingLog();
				if (processingLog != null) {
					processingLog.setSentViaNetwork();
				} else {
					this.currentEnvelope.getBuffer().recycleBuffer();
				}
			}

			synchronized (this.queuedEnvelopes) {
				this.queuedEnvelopes.poll();
				this.currentEnvelope = null;
			}
		}

		return true;
	}

	public void closeConnection(SocketChannel socketChannel, SelectionKey key) throws IOException {

		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty()) {
				socketChannel.close();
				key.cancel();
				this.isConnected = false;
			}
		}
	}

	public int getNumberOfQueuedEnvelopesForChannel(ChannelID channelID, boolean source) {

		synchronized (this.queuedEnvelopes) {

			int number = 0;

			final Iterator<TransferEnvelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {
				final TransferEnvelope te = it.next();
				if (source) {
					if (channelID.equals(te.getSource())) {
						number++;
					}
				} else {
					if (channelID.equals(te.getTarget())) {
						number++;
					}
				}
			}

			return number;
		}
	}

	public void dropAllQueuedEnvelopesForChannel(ChannelID channelID, boolean source) {

		synchronized (this.queuedEnvelopes) {

			final Iterator<TransferEnvelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {
				final TransferEnvelope te = it.next();
				if ((source && channelID.equals(te.getSource())) || (!source && channelID.equals(te.getTarget()))) {
					it.remove();
					if (te.getBuffer() != null) {
						te.getBuffer().recycleBuffer();
					}
				}
			}
		}
	}

	public int getTotalNumberOfQueuedEnvelopes() {
		synchronized (this.queuedEnvelopes) {
			return this.queuedEnvelopes.size();
		}
	}
}
