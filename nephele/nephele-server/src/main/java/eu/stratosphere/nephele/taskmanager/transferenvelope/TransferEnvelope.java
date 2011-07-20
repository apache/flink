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

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

public final class TransferEnvelope {

	private final JobID jobID;

	private final ChannelID source;

	private final int sequenceNumber;

	private final EventList eventList;

	private Buffer buffer = null;

	public TransferEnvelope(int sequenceNumber, JobID jobID, ChannelID source) {
		this(sequenceNumber, jobID, source, new EventList());
	}

	public TransferEnvelope(int sequenceNumber, JobID jobID, ChannelID source, EventList eventList) {

		this.sequenceNumber = sequenceNumber;
		this.jobID = jobID;
		this.source = source;
		this.eventList = eventList;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public ChannelID getSource() {
		return this.source;
	}

	public void addEvent(AbstractEvent event) {

		this.eventList.add(event);
	}

	public EventList getEventList() {

		return this.eventList;
	}

	public int getSequenceNumber() {
		return this.sequenceNumber;
	}

	public void setBuffer(Buffer buffer) {
		this.buffer = buffer;
	}

	public Buffer getBuffer() {
		return this.buffer;
	}

	public TransferEnvelope duplicate() {

		final TransferEnvelope duplicatedTransferEnvelope = new TransferEnvelope(this.sequenceNumber, this.jobID,
			this.source, this.eventList); // No need to duplicate event list

		if (this.buffer != null) {
			duplicatedTransferEnvelope.buffer = this.buffer.duplicate();
		} else {
			duplicatedTransferEnvelope.buffer = null;
		}

		return duplicatedTransferEnvelope;
	}

	public TransferEnvelope duplicateWithoutBuffer() {

		final TransferEnvelope duplicatedTransferEnvelope = new TransferEnvelope(this.sequenceNumber, this.jobID,
			this.source, this.eventList); // No need to duplicate event list

		duplicatedTransferEnvelope.buffer = null;

		return duplicatedTransferEnvelope;
	}
}
