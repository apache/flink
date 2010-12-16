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

package eu.stratosphere.nephele.event.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * An event list is an auxiliary object which can store and serialize/deserialize a
 * series of {@link AbstractEvent} objects.
 * 
 * @author warneke
 * @param <E>
 *        the type of {@link AbstractEvent} objects contained in the event list
 */
public class EventList<E extends AbstractEvent> implements IOReadableWritable {

	/**
	 * Stores the events contained in this list.
	 */
	private final List<E> events = new ArrayList<E>();

	/**
	 * Adds a event to the event list.
	 * 
	 * @param event
	 *        the event to be added
	 */
	public void addEvent(E event) {
		this.events.add(event);
	}

	/**
	 * Returns an {@link Iterator} object to all events
	 * stored in the event list.
	 * 
	 * @return an {@link Iterator} object to all events stores in the event list
	 */
	public Iterator<E> getEvents() {
		return this.events.iterator();
	}

	/**
	 * Returns the number of events stored in this event list.
	 * 
	 * @return the number of events stored in this event list
	 */
	public int size() {
		return this.events.size();
	}

	/**
	 * Checks whether the event list is empty.
	 * 
	 * @return <code>true</code> if the event list is empty, <code>false</code> otherwise
	 */
	public boolean isEmpty() {
		return this.events.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {

		final int numberOfEvents = in.readInt();
		for (int i = 0; i < numberOfEvents; i++) {

			final String className = StringRecord.readString(in);
			if (className == null) {
				throw new IOException("Cannot deserialize event, class name is null");
			}

			Class<? extends AbstractEvent> clazz;

			try {
				clazz = (Class<? extends AbstractEvent>) Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			E event;

			try {
				event = (E) clazz.newInstance();
			} catch (InstantiationException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (IllegalAccessException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			event.read(in);
			this.events.add(event);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		// Write out number of events
		out.writeInt(this.events.size());

		final Iterator<E> it = this.events.iterator();
		while (it.hasNext()) {
			final AbstractEvent event = it.next();
			// Write out type of event
			StringRecord.writeString(out, event.getClass().getName());
			// Write out event itself
			event.write(out);
		}
	}

}
