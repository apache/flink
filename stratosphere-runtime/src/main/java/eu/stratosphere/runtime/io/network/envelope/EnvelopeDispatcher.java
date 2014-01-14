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

package eu.stratosphere.runtime.io.network.envelope;

import java.io.IOException;

/**
 * A envelope dispatcher receives {@link Envelope}s and sends them to all of its destinations.
 */
public interface EnvelopeDispatcher {

	/**
	 * Dispatches an envelope from an output channel to the receiving input channels (forward flow).
	 *
	 * @param envelope envelope to be sent
	 */
	void dispatchFromOutputChannel(Envelope envelope) throws IOException, InterruptedException;

	/**
	 * Dispatches an envelope from an input channel to the receiving output channels (backwards flow).
	 *
	 * @param envelope envelope to be sent
	 */
	void dispatchFromInputChannel(Envelope envelope) throws IOException, InterruptedException;

	/**
	 * Dispatches an envelope from an incoming TCP connection.
	 * <p>
	 * After an envelope has been constructed from a TCP socket, this method is called to send the envelope to the
	 * receiving input channel.
	 *
	 * @param envelope envelope to be sent
	 */
	void dispatchFromNetwork(Envelope envelope) throws IOException, InterruptedException;
}
