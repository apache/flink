/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.runtime.io.network.tcp;

import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.network.RemoteReceiver;
import eu.stratosphere.runtime.io.network.envelope.Envelope;
import eu.stratosphere.runtime.io.network.envelope.EnvelopeWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * This class represents an outgoing TCP connection through which {@link eu.stratosphere.runtime.io.network.envelope.Envelope} objects can be sent.
 * {@link eu.stratosphere.runtime.io.network.envelope.Envelope} objects are received from the {@link eu.stratosphere.nephele.taskmanager.io.bytebuffered.ChannelManager} and added to a queue. An
 * additional network thread then takes the envelopes from the queue and transmits them to the respective destination
 * host.
 * 
 */
public class OutgoingConnection {

	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(OutgoingConnection.class);

	/**
	 * The address this outgoing connection is connected to.
	 */
	private final RemoteReceiver remoteReceiver;

	/**
	 * The outgoing connection thread which actually transmits the queued transfer envelopes.
	 */
	private final OutgoingConnectionThread connectionThread;

	/**
	 * The queue of transfer envelopes to be transmitted.
	 */
	private final Queue<Envelope> queuedEnvelopes = new ArrayDeque<Envelope>();

	/**
	 * The {@link eu.stratosphere.runtime.io.network.envelope.Envelope} that is currently processed.
	 */
	private Envelope currentEnvelope = null;

	/**
	 * Stores whether the underlying TCP connection is established. As this variable is accessed by the byte buffered
	 * channel manager and the outgoing connection thread, it must be protected by a monitor.
	 */
	private boolean isConnected = false;

	/**
	 * Stores whether is underlying TCP connection is subscribed to the NIO write event. As this variable is accessed by
	 * the byte buffered channel and the outgoing connection thread, it must be protected by a monitor.
	 */
	private boolean isSubscribedToWriteEvent = false;

	/**
	 * The overall number of connection retries which shall be performed before a connection error is reported.
	 */
	private final int numberOfConnectionRetries;

	/**
	 * The number of connection retries left before an I/O error is reported.
	 */
	private int retriesLeft = 0;

	/**
	 * The timestamp of the last connection retry.
	 */
	private long timstampOfLastRetry = 0;

	/**
	 * The current selection key representing the interest set of the underlying TCP NIO connection. This variable may
	 * only be accessed the the outgoing connection thread.
	 */
	private SelectionKey selectionKey = null;

	/**
	 * The period of time in milliseconds that shall be waited before a connection attempt is considered to be failed.
	 */
	private static long RETRYINTERVAL = 1000L; // 1 second

	private EnvelopeWriter writer;

	/**
	 * Constructs a new outgoing connection object.
	 * 
	 * @param remoteReceiver
	 *        the address of the destination host this outgoing connection object is supposed to connect to
	 * @param connectionThread
	 *        the connection thread which actually handles the network transfer
	 * @param numberOfConnectionRetries
	 *        the number of connection retries allowed before an I/O error is reported
	 */
	public OutgoingConnection(RemoteReceiver remoteReceiver, OutgoingConnectionThread connectionThread,
			int numberOfConnectionRetries) {

		this.remoteReceiver = remoteReceiver;
		this.connectionThread = connectionThread;
		this.numberOfConnectionRetries = numberOfConnectionRetries;
		this.writer = new EnvelopeWriter();
	}

	/**
	 * Adds a new {@link eu.stratosphere.runtime.io.network.envelope.Envelope} to the queue of envelopes to be transmitted to the destination host of this
	 * connection.
	 * <p>
	 * This method should only be called by the {@link eu.stratosphere.nephele.taskmanager.io.bytebuffered.ChannelManager} object.
	 * 
	 * @param envelope
	 *        the envelope to be added to the transfer queue
	 */
	public void queueEnvelope(Envelope envelope) {

		synchronized (this.queuedEnvelopes) {

			checkConnection();
			this.queuedEnvelopes.add(envelope);
		}
	}

	private void checkConnection() {

		synchronized (this.queuedEnvelopes) {

			if (!this.isConnected) {

				this.retriesLeft = this.numberOfConnectionRetries;
				this.timstampOfLastRetry = System.currentTimeMillis();
				this.connectionThread.triggerConnect(this);
				this.isConnected = true;
				this.isSubscribedToWriteEvent = true;
			} else {

				if (!this.isSubscribedToWriteEvent) {
					this.connectionThread.subscribeToWriteEvent(this.selectionKey);
					this.isSubscribedToWriteEvent = true;
				}
			}

		}
	}

	/**
	 * Returns the {@link InetSocketAddress} to the destination host this outgoing connection is supposed to be
	 * connected to.
	 * <p>
	 * This method should be called by the {@link OutgoingConnectionThread} object only.
	 * 
	 * @return the {@link InetSocketAddress} to the destination host this outgoing connection is supposed to be
	 *         connected to
	 */
	public InetSocketAddress getConnectionAddress() {

		return this.remoteReceiver.getConnectionAddress();
	}

	/**
	 * Reports a problem which occurred while establishing the underlying TCP connection to this outgoing connection
	 * object. Depending on the number of connection retries left, this method will either try to reestablish the TCP
	 * connection or report an I/O error to all tasks which have queued envelopes for this connection. In the latter
	 * case all queued envelopes will be dropped and all included buffers will be freed.
	 * <p>
	 * This method should only be called by the {@link OutgoingConnectionThread} object.
	 * 
	 * @param ioe
	 *        thrown if an error occurs while reseting the underlying TCP connection
	 */
	public void reportConnectionProblem(IOException ioe) {

		// First, write exception to log
		final long currentTime = System.currentTimeMillis();
		if (currentTime - this.timstampOfLastRetry >= RETRYINTERVAL) {
			LOG.error("Cannot connect to " + this.remoteReceiver + ", " + this.retriesLeft + " retries left");
		}

		synchronized (this.queuedEnvelopes) {

			if (this.selectionKey != null) {

				final SocketChannel socketChannel = (SocketChannel) this.selectionKey.channel();
				if (socketChannel != null) {
					try {
						socketChannel.close();
					} catch (IOException e) {
						LOG.debug("Error while trying to close the socket channel to " + this.remoteReceiver);
					}
				}

				this.selectionKey.cancel();
				this.selectionKey = null;
				this.isConnected = false;
				this.isSubscribedToWriteEvent = false;
			}

			if (hasRetriesLeft(currentTime)) {
				this.connectionThread.triggerConnect(this);
				this.isConnected = true;
				this.isSubscribedToWriteEvent = true;
				return;
			}

			// Error is fatal
			LOG.error(ioe);

			// Notify source of current envelope and release buffer
			if (this.currentEnvelope != null) {
				if (this.currentEnvelope.getBuffer() != null) {
					this.currentEnvelope.getBuffer().recycleBuffer();
					this.currentEnvelope = null;
				}
			}

			// Notify all other tasks which are waiting for data to be transmitted
			final Iterator<Envelope> iter = this.queuedEnvelopes.iterator();
			while (iter.hasNext()) {
				final Envelope envelope = iter.next();
				iter.remove();
				// Recycle the buffer inside the envelope
				if (envelope.getBuffer() != null) {
					envelope.getBuffer().recycleBuffer();
				}
			}

			this.queuedEnvelopes.clear();
		}
	}

	/**
	 * Reports an I/O error which occurred while writing data to the TCP connection. As a result of the I/O error the
	 * connection is closed and the interest keys are canceled. Moreover, the task which queued the currently
	 * transmitted transfer envelope is notified about the error and the current envelope is dropped. If the current
	 * envelope contains a buffer, the buffer is freed.
	 * <p>
	 * This method should only be called by the {@link OutgoingConnectionThread} object.
	 * 
	 * @param ioe
	 *        thrown if an error occurs while reseting the connection
	 */
	public void reportTransmissionProblem(IOException ioe) {

		final SocketChannel socketChannel = (SocketChannel) this.selectionKey.channel();

		// First, write exception to log
		if (this.currentEnvelope != null) {
			LOG.error("The connection between " + socketChannel.socket().getLocalAddress() + " and "
				+ socketChannel.socket().getRemoteSocketAddress()
				+ " experienced an IOException for transfer envelope " + this.currentEnvelope.getSequenceNumber());
		} else {
			LOG.error("The connection between " + socketChannel.socket().getLocalAddress() + " and "
				+ socketChannel.socket().getRemoteSocketAddress() + " experienced an IOException");
		}

		// Close the connection and cancel the interest key
		synchronized (this.queuedEnvelopes) {
			try {
				LOG.debug("Closing connection to " + socketChannel.socket().getRemoteSocketAddress());
				socketChannel.close();
			} catch (IOException e) {
				LOG.debug("An error occurred while responding to an IOException");
				LOG.debug(e);
			}

			this.selectionKey.cancel();

			// Error is fatal
			LOG.error(ioe);

			// Trigger new connection if there are more envelopes to be transmitted
			if (this.queuedEnvelopes.isEmpty()) {
				this.isConnected = false;
				this.isSubscribedToWriteEvent = false;
			} else {
				this.connectionThread.triggerConnect(this);
				this.isConnected = true;
				this.isSubscribedToWriteEvent = true;
			}

			// We must assume the current envelope is corrupted so we notify the task which created it.
			if (this.currentEnvelope != null) {
				if (this.currentEnvelope.getBuffer() != null) {
					this.currentEnvelope.getBuffer().recycleBuffer();
					this.currentEnvelope = null;
				}
			}
		}
	}

	/**
	 * Checks whether further retries are left for establishing the underlying TCP connection.
	 * 
	 * @param currentTime
	 *        the current system time in milliseconds since January 1st, 1970
	 * @return <code>true</code> if there are retries left, <code>false</code> otherwise
	 */
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

	/**
	 * Writes the content of the current {@link eu.stratosphere.runtime.io.network.envelope.Envelope} object to the underlying TCP connection.
	 * <p>
	 * This method should only be called by the {@link OutgoingConnectionThread} object.
	 * 
	 * @return <code>true</code> if there is more data from this/other queued envelopes to be written to this channel
	 * @throws IOException
	 *         thrown if an error occurs while writing the data to the channel
	 */
	public boolean write() throws IOException {

		final WritableByteChannel writableByteChannel = (WritableByteChannel) this.selectionKey.channel();

		if (this.currentEnvelope == null) {
			synchronized (this.queuedEnvelopes) {
				if (this.queuedEnvelopes.isEmpty()) {
					return false;
				} else {
					this.currentEnvelope = this.queuedEnvelopes.peek();

					this.writer.setEnvelopeForWriting(this.currentEnvelope);
				}
			}
		}

		if (!this.writer.writeNextChunk(writableByteChannel)) {
			// Make sure we recycle the attached memory or file buffers correctly
			if (this.currentEnvelope.getBuffer() != null) {
				this.currentEnvelope.getBuffer().recycleBuffer();
			}

			synchronized (this.queuedEnvelopes) {
				this.queuedEnvelopes.poll();
				this.currentEnvelope = null;
			}
		}

		return true;
	}

	/**
	 * Requests to close the underlying TCP connection. The request is ignored if at least one {@link eu.stratosphere.runtime.io.network.envelope.Envelope}
	 * is queued.
	 * <p>
	 * This method should only be called by the {@link OutgoingConnectionThread} object.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while closing the TCP connection
	 */
	public void requestClose() throws IOException {

		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty()) {

				if (this.isSubscribedToWriteEvent) {

					this.connectionThread.unsubscribeFromWriteEvent(this.selectionKey);
					this.isSubscribedToWriteEvent = false;
				}
			}
		}
	}

	/**
	 * Closes the underlying TCP connection if no more {@link eu.stratosphere.runtime.io.network.envelope.Envelope} objects are in the transmission queue.
	 * <p>
	 * This method should only be called by the {@link OutgoingConnectionThread} object.
	 * 
	 * @throws IOException
	 */
	public void closeConnection() throws IOException {

		synchronized (this.queuedEnvelopes) {

			if (!this.queuedEnvelopes.isEmpty()) {
				return;
			}

			if (this.selectionKey != null) {

				final SocketChannel socketChannel = (SocketChannel) this.selectionKey.channel();
				socketChannel.close();
				this.selectionKey.cancel();
				this.selectionKey = null;
			}

			this.isConnected = false;
			this.isSubscribedToWriteEvent = false;
		}
	}

	/**
	 * Returns the number of queued {@link eu.stratosphere.runtime.io.network.envelope.Envelope} objects with the given source channel ID.
	 * 
	 * @param sourceChannelID
	 *        the source channel ID to count the queued envelopes for
	 * @return the number of queued transfer envelopes with the given source channel ID
	 */
	public int getNumberOfQueuedEnvelopesFromChannel(final ChannelID sourceChannelID) {

		synchronized (this.queuedEnvelopes) {

			int number = 0;

			final Iterator<Envelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {
				final Envelope te = it.next();
				if (sourceChannelID.equals(te.getSource())) {
					number++;
				}
			}

			return number;
		}
	}

	/**
	 * Removes all queued {@link eu.stratosphere.runtime.io.network.envelope.Envelope} objects from the transmission which match the given source channel
	 * ID.
	 * 
	 * @param sourceChannelID
	 *        the source channel ID of the transfered transfer envelopes to be dropped
	 */
	public void dropAllQueuedEnvelopesFromChannel(final ChannelID sourceChannelID) {

		synchronized (this.queuedEnvelopes) {

			final Iterator<Envelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {
				final Envelope te = it.next();
				if (sourceChannelID.equals(te.getSource())) {
					it.remove();
					if (te.getBuffer() != null) {
						te.getBuffer().recycleBuffer();
					}
				}
			}
		}
	}

	/**
	 * Checks whether this outgoing connection object manages an active connection or can be removed by the
	 * {@link eu.stratosphere.nephele.taskmanager.io.bytebuffered.ChannelManager} object.
	 * <p>
	 * This method should only be called by the byte buffered channel manager.
	 * 
	 * @return <code>true</code> if this object is no longer manages an active connection and can be removed,
	 *         <code>false</code> otherwise.
	 */
	public boolean canBeRemoved() {

		synchronized (this.queuedEnvelopes) {

			if (this.isConnected) {
				return false;
			}

			if (this.currentEnvelope != null) {
				return false;
			}

			return this.queuedEnvelopes.isEmpty();
		}
	}

	/**
	 * Sets the selection key representing the interest set of the underlying TCP NIO connection.
	 * 
	 * @param selectionKey
	 *        the selection of the underlying TCP connection
	 */
	public void setSelectionKey(SelectionKey selectionKey) {
		this.selectionKey = selectionKey;
	}

	/**
	 * Returns the number of currently queued envelopes which contain a write buffer.
	 * 
	 * @return the number of currently queued envelopes which contain a write buffer
	 */
	public int getNumberOfQueuedWriteBuffers() {

		int retVal = 0;

		synchronized (this.queuedEnvelopes) {

			final Iterator<Envelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {

				final Envelope envelope = it.next();
				if (envelope.getBuffer() != null) {
					++retVal;
				}
			}
		}

		return retVal;
	}
}
