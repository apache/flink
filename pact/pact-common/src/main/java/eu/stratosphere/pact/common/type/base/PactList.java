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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

public abstract class PactList<V extends Value> implements Value, List<V> {
	private final Class<V> valueClass;

	private final List<V> list;

	public PactList() {
		this.valueClass = ReflectionUtil.<V> getTemplateType1(this.getClass());

		this.list = new ArrayList<V>();
	}

	public PactList(final Collection<V> c) {
		this.valueClass = ReflectionUtil.<V> getTemplateType1(this.getClass());

		this.list = new ArrayList<V>(c);
	}

	@Override
	public Iterator<V> iterator() {
		return this.list.iterator();
	}

	@Override
	public void read(final DataInput in) throws IOException {
		int size = in.readInt();
		this.list.clear();

		try {
			for (; size > 0; size--) {
				final V val = this.valueClass.newInstance();
				val.read(in);

				this.list.add(val);
			}
		} catch (final InstantiationException e) {
			throw new RuntimeException(e);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(this.list.size());
		for (final V value : this.list)
			value.write(out);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 41;
		int result = 1;
		result = prime * result + (this.list == null ? 0 : this.list.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final PactList<?> other = (PactList<?>) obj;
		if (this.list == null) {
			if (other.list != null)
				return false;
		} else if (!this.list.equals(other.list))
			return false;
		return true;
	}

	public void add(final int index, final V element) {
		this.list.add(index, element);
	}

	public boolean add(final V e) {
		return this.list.add(e);
	}

	public boolean addAll(final Collection<? extends V> c) {
		return this.list.addAll(c);
	}

	public boolean addAll(final int index, final Collection<? extends V> c) {
		return this.list.addAll(index, c);
	}

	public void clear() {
		this.list.clear();
	}

	public boolean contains(final Object o) {
		return this.list.contains(o);
	}

	public boolean containsAll(final Collection<?> c) {
		return this.list.containsAll(c);
	}

	public V get(final int index) {
		return this.list.get(index);
	}

	public int indexOf(final Object o) {
		return this.list.indexOf(o);
	}

	public boolean isEmpty() {
		return this.list.isEmpty();
	}

	public int lastIndexOf(final Object o) {
		return this.list.lastIndexOf(o);
	}

	public ListIterator<V> listIterator() {
		return this.list.listIterator();
	}

	public ListIterator<V> listIterator(final int index) {
		return this.list.listIterator(index);
	}

	public V remove(final int index) {
		return this.list.remove(index);
	}

	public boolean remove(final Object o) {
		return this.list.remove(o);
	}

	public boolean removeAll(final Collection<?> c) {
		return this.list.removeAll(c);
	}

	public boolean retainAll(final Collection<?> c) {
		return this.list.retainAll(c);
	}

	public V set(final int index, final V element) {
		return this.list.set(index, element);
	}

	public int size() {
		return this.list.size();
	}

	public List<V> subList(final int fromIndex, final int toIndex) {
		return this.list.subList(fromIndex, toIndex);
	}

	public Object[] toArray() {
		return this.list.toArray();
	}

	public <T> T[] toArray(final T[] a) {
		return this.list.toArray(a);
	}

}
