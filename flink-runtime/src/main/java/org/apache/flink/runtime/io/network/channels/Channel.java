/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.io.network.channels;

import java.io.IOException;

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.EnvelopeDispatcher;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * The base class for channel objects.
 * <p>
 * Every channel has an index (at the corresponding gate), ID, and type. The connected channel is given by the ID of
 * destination channel.
 */
public abstract class Channel {

	private final ChannelID id;

	private final ChannelID connectedId;

	private final int index;

	private final ChannelType type;

	protected EnvelopeDispatcher envelopeDispatcher;

	/**
	 * Auxiliary constructor for channels
	 * 
	 * @param index the index of the channel in either the output or input gate
	 * @param id the ID of the channel
	 * @param connectedId the ID of the channel this channel is connected to
	 */
	protected Channel(int index, ChannelID id, ChannelID connectedId, ChannelType type) {
		this.index = index;
		this.id = id;
		this.connectedId = connectedId;
		this.type = type;
	}

	public int getIndex() {
		return this.index;
	}

	public ChannelID getID() {
		return this.id;
	}

	public ChannelID getConnectedId() {
		return this.connectedId;
	}

	public ChannelType getChannelType() {
		return this.type;
	}

	/**
	 * Registers an EnvelopeDispatcher with this channel at runtime.
	 *
	 * @param envelopeDispatcher the envelope dispatcher to use for data transfers
	 */
	public void registerEnvelopeDispatcher(EnvelopeDispatcher envelopeDispatcher) {
		this.envelopeDispatcher = envelopeDispatcher;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public abstract JobID getJobID();

	public abstract boolean isInputChannel();

	public abstract boolean isClosed() throws IOException, InterruptedException;

	public abstract void transferEvent(AbstractEvent event) throws IOException, InterruptedException;

	public abstract void queueEnvelope(Envelope envelope);

	// nothing to do for buffer oriented runtime => TODO remove with pending changes for input side
	public abstract void releaseAllResources();

	// nothing to do for buffer oriented runtime => TODO remove with pending changes for input side
	public abstract void destroy();
}
