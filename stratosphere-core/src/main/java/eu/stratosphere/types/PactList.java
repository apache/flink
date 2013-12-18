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

package eu.stratosphere.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.util.ReflectionUtil;

/**
 * Generic list base type for PACT programs that implements the Value and List interfaces.
 * PactList encapsulates a Java ArrayList object.
 * 
 * @see eu.stratosphere.types.Value
 * @see java.util.List
 * @see java.util.ArrayList
 * 
 * @param <V> Type of the list elements.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * 
 */
public abstract class PactList<V extends Value> implements Value, List<V> {
	private static final long serialVersionUID = 1L;
	
	// Type of list elements
	private final Class<V> valueClass;
	// Encapsulated list
	private final List<V> list;

	/**
	 * Initializes the encapsulated list with an empty ArrayList.
	 * 
	 * @see java.util.ArrayList
	 */
	public PactList() {
		this.valueClass = ReflectionUtil.<V> getTemplateType1(this.getClass());

		this.list = new ArrayList<V>();
	}

	/**
	 * Initializes the encapsulated list with an ArrayList filled with all object contained in the specified Collection object.
	 * 
	 * @see java.util.ArrayList
	 * @see java.util.Collection
	 * 
	 * @param c Collection of initial element of the encapsulated list.
	 */
	public PactList(final Collection<V> c) {
		this.valueClass = ReflectionUtil.<V> getTemplateType1(this.getClass());

		this.list = new ArrayList<V>(c);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#iterator()
	 */
	@Override
	public Iterator<V> iterator() {
		return this.list.iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see java.util.List#add(int, java.lang.Object)
	 */
	@Override
	public void add(final int index, final V element) {
		this.list.add(index, element);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#add(java.lang.Object)
	 */
	@Override
	public boolean add(final V e) {
		return this.list.add(e);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#addAll(java.util.Collection)
	 */
	@Override
	public boolean addAll(final Collection<? extends V> c) {
		return this.list.addAll(c);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#addAll(int, java.util.Collection)
	 */
	@Override
	public boolean addAll(final int index, final Collection<? extends V> c) {
		return this.list.addAll(index, c);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#clear()
	 */
	@Override
	public void clear() {
		this.list.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#contains(java.lang.Object)
	 */
	@Override
	public boolean contains(final Object o) {
		return this.list.contains(o);
	}
	
	@Override
	public String toString() {
		return this.list.toString();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.List#containsAll(java.util.Collection)
	 */
	@Override
	public boolean containsAll(final Collection<?> c) {
		return this.list.containsAll(c);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#get(int)
	 */
	@Override
	public V get(final int index) {
		return this.list.get(index);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#indexOf(java.lang.Object)
	 */
	@Override
	public int indexOf(final Object o) {
		return this.list.indexOf(o);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.list.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#lastIndexOf(java.lang.Object)
	 */
	@Override
	public int lastIndexOf(final Object o) {
		return this.list.lastIndexOf(o);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#listIterator()
	 */
	@Override
	public ListIterator<V> listIterator() {
		return this.list.listIterator();
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.util.List#listIterator(int)
	 */
	@Override
	public ListIterator<V> listIterator(final int index) {
		return this.list.listIterator(index);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#remove(int)
	 */
	@Override
	public V remove(final int index) {
		return this.list.remove(index);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(final Object o) {
		return this.list.remove(o);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#removeAll(java.util.Collection)
	 */
	@Override
	public boolean removeAll(final Collection<?> c) {
		return this.list.removeAll(c);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#retainAll(java.util.Collection)
	 */
	@Override
	public boolean retainAll(final Collection<?> c) {
		return this.list.retainAll(c);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#set(int, java.lang.Object)
	 */
	@Override
	public V set(final int index, final V element) {
		return this.list.set(index, element);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#size()
	 */
	@Override
	public int size() {
		return this.list.size();
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.util.List#subList(int, int)
	 */
	@Override
	public List<V> subList(final int fromIndex, final int toIndex) {
		return this.list.subList(fromIndex, toIndex);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#toArray()
	 */
	@Override
	public Object[] toArray() {
		return this.list.toArray();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.List#toArray(T[])
	 */
	@Override
	public <T> T[] toArray(final T[] a) {
		return this.list.toArray(a);
	}

}
