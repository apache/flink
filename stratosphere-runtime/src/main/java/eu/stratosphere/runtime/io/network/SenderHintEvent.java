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

package eu.stratosphere.runtime.io.network;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.network.envelope.Envelope;

public final class SenderHintEvent extends AbstractEvent {

	/**
	 * The sequence number that will be set for transfer envelopes which contain the sender hint event.
	 */
	private static final int SENDER_HINT_SEQUENCE_NUMBER = 0;

	private final ChannelID source;

	private final RemoteReceiver remoteReceiver;

	SenderHintEvent(final ChannelID source, final RemoteReceiver remoteReceiver) {

		if (source == null) {
			throw new IllegalArgumentException("Argument source must not be null");
		}

		if (remoteReceiver == null) {
			throw new IllegalArgumentException("Argument remoteReceiver must not be null");
		}

		this.source = source;
		this.remoteReceiver = remoteReceiver;
	}

	public SenderHintEvent() {

		this.source = new ChannelID();
		this.remoteReceiver = new RemoteReceiver();
	}

	public ChannelID getSource() {

		return this.source;
	}

	public RemoteReceiver getRemoteReceiver() {

		return this.remoteReceiver;
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		this.source.write(out);
		this.remoteReceiver.write(out);
	}


	@Override
	public void read(final DataInput in) throws IOException {

		this.source.read(in);
		this.remoteReceiver.read(in);
	}

	public static Envelope createEnvelopeWithEvent(final Envelope originalEnvelope,
			final ChannelID source, final RemoteReceiver remoteReceiver) {

		final Envelope envelope = new Envelope(SENDER_HINT_SEQUENCE_NUMBER,
			originalEnvelope.getJobID(), originalEnvelope.getSource());

		final SenderHintEvent senderEvent = new SenderHintEvent(source, remoteReceiver);
		envelope.serializeEventList(Arrays.asList(senderEvent));

		return envelope;
	}

	static boolean isSenderHintEvent(final Envelope envelope) {

		if (envelope.getSequenceNumber() != SENDER_HINT_SEQUENCE_NUMBER) {
			return false;
		}

		if (envelope.getBuffer() != null) {
			return false;
		}

		List<? extends AbstractEvent> events = envelope.deserializeEvents();

		if (events.size() != 1) {
			return false;
		}

		if (!(events.get(0) instanceof SenderHintEvent)) {
			return false;
		}

		return true;
	}
}
