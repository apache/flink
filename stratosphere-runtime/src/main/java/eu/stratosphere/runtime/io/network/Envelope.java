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

package eu.stratosphere.runtime.io.network;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.runtime.io.serialization.DataInputDeserializer;
import eu.stratosphere.runtime.io.serialization.DataOutputSerializer;
import eu.stratosphere.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Envelope {

	private final JobID jobID;

	private final ChannelID source;

	private final int sequenceNumber;

	private ByteBuffer serializedEventList;

	private Buffer buffer;

	public Envelope(int sequenceNumber, JobID jobID, ChannelID source) {
		this.sequenceNumber = sequenceNumber;
		this.jobID = jobID;
		this.source = source;
	}

	private Envelope(Envelope toDuplicate) {
		this.jobID = toDuplicate.jobID;
		this.source = toDuplicate.source;
		this.sequenceNumber = toDuplicate.sequenceNumber;
		this.serializedEventList = null;
		this.buffer = null;
	}

	public Envelope duplicate() {
		Envelope duplicate = new Envelope(this);
		if (hasBuffer()) {
			duplicate.setBuffer(this.buffer.duplicate());
		}

		return duplicate;
	}

	public Envelope duplicateWithoutBuffer() {
		return new Envelope(this);
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public ChannelID getSource() {
		return this.source;
	}

	public int getSequenceNumber() {
		return this.sequenceNumber;
	}

	public void setEventsSerialized(ByteBuffer serializedEventList) {
		if (this.serializedEventList != null) {
			throw new IllegalStateException("Event list has already been set.");
		}

		this.serializedEventList = serializedEventList;
	}

	public void serializeEventList(List<? extends AbstractEvent> eventList) {
		if (this.serializedEventList != null) {
			throw new IllegalStateException("Event list has already been set.");
		}

		this.serializedEventList = serializeEvents(eventList);
	}

	public ByteBuffer getEventsSerialized() {
		return this.serializedEventList;
	}

	public List<? extends AbstractEvent> deserializeEvents() {
		return deserializeEvents(getClass().getClassLoader());
	}

	public List<? extends AbstractEvent> deserializeEvents(ClassLoader classloader) {
		if (this.serializedEventList == null) {
			return Collections.emptyList();
		}

		try {
			DataInputDeserializer deserializer = new DataInputDeserializer(this.serializedEventList);

			int numEvents = deserializer.readInt();
			ArrayList<AbstractEvent> events = new ArrayList<AbstractEvent>(numEvents);

			for (int i = 0; i < numEvents; i++) {
				String className = deserializer.readUTF();
				Class<? extends AbstractEvent> clazz;
				try {
					clazz = Class.forName(className).asSubclass(AbstractEvent.class);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException("Could not load event class '" + className + "'.", e);
				} catch (ClassCastException e) {
					throw new RuntimeException("The class '" + className + "' is no valid subclass of '" + AbstractEvent.class.getName() + "'.", e);
				}

				AbstractEvent evt = InstantiationUtil.instantiate(clazz, AbstractEvent.class);
				evt.read(deserializer);

				events.add(evt);
			}

			return events;
		}
		catch (IOException e) {
			throw new RuntimeException("Error while deserializing the events.", e);
		}
	}

	public void setBuffer(Buffer buffer) {
		this.buffer = buffer;
	}

	public Buffer getBuffer() {
		return this.buffer;
	}

	private ByteBuffer serializeEvents(List<? extends AbstractEvent> events) {
		try {
			// create the serialized event list
			DataOutputSerializer serializer = events.size() == 0
				? new DataOutputSerializer(4)
				: new DataOutputSerializer(events.size() * 32);
			serializer.writeInt(events.size());

			for (AbstractEvent evt : events) {
				serializer.writeUTF(evt.getClass().getName());
				evt.write(serializer);
			}

			return serializer.wrapAsByteBuffer();
		}
		catch (IOException e) {
			throw new RuntimeException("Error while serializing the task events.", e);
		}
	}

	public boolean hasBuffer() {
		return this.buffer != null;
	}

	@Override
	public String toString() {
		return String.format("Envelope %d [source id: %s, buffer size: %d, events size: %d]",
				this.sequenceNumber, this.getSource(), this.buffer == null ? -1 : this.buffer.size(),
				this.serializedEventList == null ? -1 : this.serializedEventList.remaining());
	}
}
