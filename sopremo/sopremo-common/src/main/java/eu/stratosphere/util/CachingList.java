/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * A list implementation that does not release removed elements but allows them to be reused.
 * 
 * @author Arvid Heise
 */
public class CachingList<T> extends AbstractList<T> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3096573686030611189L;

	private List<T> backingList = new ArrayList<T>();

	private int size;

	@Override
	public void add(int index, T element) {
		checkRange(index, this.size + 1);

		this.size++;
		this.backingList.add(index, element);
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		this.backingList = (List<T>) ois.readObject();
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.writeObject(new ArrayList<T>(this.backingList.subList(0, this.size)));
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractList#clear()
	 */
	@Override
	public void clear() {
		this.size = 0;
	}

	@Override
	public T remove(int index) {
		checkRange(index, this.size);

		final T oldObject = this.backingList.remove(index);
		this.backingList.add(oldObject);
		this.size--;
		return oldObject;
	}

	private void checkRange(int index, int size) {
		if (index >= size)
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractList#get(int)
	 */
	@Override
	public T get(int index) {
		checkRange(index, this.size);

		return this.backingList.get(index);
	}

	public T reuseUnusedElement() {
		if (this.backingList.size() == this.size)
			return null;
		return this.backingList.get(this.size++);
	}
	
	/**
	 * Sets the size to the specified value.
	 * 
	 * @param size
	 *        the size to set
	 */
	public void setSize(int size) {
		if (size < 0)
			throw new NullPointerException("size must not be non-negative");

		CollectionUtil.ensureSize(this.backingList, size);
		this.size = size;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#size()
	 */
	@Override
	public int size() {
		return this.size;
	}

}
