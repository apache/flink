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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * A transfer envelope dispatcher receives {@link TransferEnvelopes} and sends them to all of this destinations.
 * 
 * @author warneke
 */
public interface TransferEnvelopeDispatcher {

	/**
	 * Processes a transfer envelope from an output channel. The method may block until the system has allocated enough
	 * resources to further process the envelope.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be processed
	 */
	void processEnvelopeFromOutputChannel(TransferEnvelope transferEnvelope);

	void processEnvelopeFromInputChannel(TransferEnvelope transferEnvelope);

	void processEnvelopeFromNetwork(TransferEnvelope transferEnvelope, boolean freeSourceBuffer);

	/**
	 * Registers the given spilling queue with a network connection. The network connection is in charge of polling the
	 * remaining elements from the queue.
	 * 
	 * @param jobID
	 *        the ID of the job which is associated with the spilling queue
	 * @param sourceChannelID
	 *        the ID of the source channel which is associated with the spilling queue
	 * @param spillingQueue
	 *        the spilling queue to be registered
	 * @return <code>true</code> if the has been successfully registered with the network connection, <code>false</code>
	 *         if the receiver runs within the same task manager and there is no network operation required to transfer
	 *         the queued data
	 * @throws IOException
	 *         thrown if an I/O error occurs while looking up the destination of the queued envelopes
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while looking up the destination of the queued envelopes
	 */
	boolean registerSpillingQueueWithNetworkConnection(final JobID jobID, final ChannelID sourceChannelID,
			final SpillingQueue spillingQueue) throws IOException, InterruptedException;
}
