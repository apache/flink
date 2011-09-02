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

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;

public final class TransferEnvelope {

	private ChannelID source = null;

	private ChannelID target = null;

	private EventList eventList;

	private int sequenceNumber = -1;

	private Buffer buffer = null;

	private final TransferEnvelopeProcessingLog processingLog;

	public TransferEnvelope(ChannelID source, ChannelID target, TransferEnvelopeProcessingLog processingLog) {
		this.source = source;
		this.target = target;
		this.processingLog = processingLog;
	}

	public TransferEnvelope() {
		this.processingLog = null;
	}

	public void setSource(ChannelID source) {
		this.source = source;
	}

	public ChannelID getSource() {
		return this.source;
	}

	public void setTarget(ChannelID target) {
		this.target = target;
	}

	public ChannelID getTarget() {
		return this.target;
	}

	public void addEvent(AbstractEvent event) {

		if (this.eventList == null) {
			this.eventList = new EventList();
		}

		this.eventList.add(event);
	}

	public EventList getEventList() {

		if (this.eventList == null) {
			this.eventList = new EventList();
		}

		return this.eventList;
	}

	public void setEventList(EventList eventList) {
		this.eventList = eventList;
	}

	public void setSequenceNumber(int sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public int getSequenceNumber() {
		return this.sequenceNumber;
	}

	public void setBuffer(Buffer buffer) {
		this.buffer = buffer;

		if (this.processingLog != null) {
			this.processingLog.setBuffer(buffer);
		}
	}

	public Buffer getBuffer() {
		return this.buffer;
	}

	public TransferEnvelopeProcessingLog getProcessingLog() {
		return this.processingLog;
	}

	public TransferEnvelope duplicate() {

		final TransferEnvelope duplicatedTransferEnvelope = new TransferEnvelope(this.source, this.target,
			this.processingLog);

		duplicatedTransferEnvelope.sequenceNumber = this.sequenceNumber;
		duplicatedTransferEnvelope.eventList = this.eventList; // No need to duplicate event list
		if (this.buffer != null) {
			duplicatedTransferEnvelope.buffer = this.buffer.duplicate();
		} else {
			duplicatedTransferEnvelope.buffer = null;
		}

		return duplicatedTransferEnvelope;
	}
	
	public String toString(){
		return "Sequenznumber " + this.sequenceNumber + "\n"
		+ "Target " + this.target + "\n" 
		+ "Source " + this.source;
		
	}
}
