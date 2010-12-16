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

package eu.stratosphere.nephele.event.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * Objects of this class can store and serialize/deserialize {@link eu.stratosphere.nephele.event.task.AbstractEvent}
 * objects.
 * 
 * @author warneke
 */
public class EventList implements List<AbstractEvent>, IOReadableWritable {

	/**
	 * Stores the events contained in this event list.
	 */
	private ArrayList<AbstractEvent> events = new ArrayList<AbstractEvent>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean add(AbstractEvent arg0) {

		return this.events.add(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void add(int arg0, AbstractEvent arg1) {

		this.events.add(arg0, arg1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addAll(Collection<? extends AbstractEvent> arg0) {

		return this.events.addAll(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addAll(int arg0, Collection<? extends AbstractEvent> arg1) {

		return addAll(arg0, arg1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {

		this.events.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(Object arg0) {

		return this.events.contains(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsAll(Collection<?> arg0) {

		return this.events.containsAll(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractEvent get(int arg0) {

		return this.events.get(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int indexOf(Object arg0) {

		return this.events.indexOf(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {

		return this.events.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<AbstractEvent> iterator() {

		return this.events.iterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int lastIndexOf(Object arg0) {

		return this.events.lastIndexOf(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ListIterator<AbstractEvent> listIterator() {

		return this.events.listIterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ListIterator<AbstractEvent> listIterator(int arg0) {

		return this.events.listIterator(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(Object arg0) {

		return this.events.remove(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractEvent remove(int arg0) {

		return this.events.remove(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAll(Collection<?> arg0) {

		return this.events.remove(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean retainAll(Collection<?> arg0) {

		return this.events.retainAll(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractEvent set(int arg0, AbstractEvent arg1) {

		return this.events.set(arg0, arg1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {

		return this.events.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AbstractEvent> subList(int arg0, int arg1) {

		return this.subList(arg0, arg1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object[] toArray() {

		return this.events.toArray();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> T[] toArray(T[] arg0) {

		return this.events.toArray(arg0);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	// TODO: See if type safety can be improved here
	@Override
	public void read(DataInput in) throws IOException {

		final int numberOfNotifications = in.readInt();

		for (int i = 0; i < numberOfNotifications; i++) {
			final String notificationType = StringRecord.readString(in);
			Class<? extends AbstractEvent> clazz = null;
			try {
				clazz = (Class<? extends AbstractEvent>) Class.forName(notificationType);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			AbstractEvent notification = null;
			try {
				notification = clazz.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}
			notification.read(in);
			this.events.add(notification);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		// Write number of items
		out.writeInt(this.events.size());

		// Write items themselves
		Iterator<AbstractEvent> it = this.events.iterator();

		while (it.hasNext()) {
			final AbstractEvent notification = it.next();
			// Write out type
			StringRecord.writeString(out, notification.getClass().getName());
			// Write out notification itself
			notification.write(out);
		}
	}

}
