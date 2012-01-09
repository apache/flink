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
package eu.stratosphere.pact.testing;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * @author Arvid Heise
 */
public class MutableObjectIteratorUnwrapper<T> implements Iterator<T> {
	private T[] buffer;

	private MutableObjectIterator<T> iterator;

	private boolean hasNext;

	private int nextIndex = 0;

	@SuppressWarnings("unchecked")
	public MutableObjectIteratorUnwrapper(MutableObjectIterator<T> iterator, Class<T> type) {
		this.iterator = iterator;
		try {
			this.buffer = (T[]) Array.newInstance(type, 2);
			this.buffer[0] = type.newInstance();
			this.buffer[1] = type.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		try {
			this.hasNext = iterator.next(this.buffer[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public MutableObjectIteratorUnwrapper(MutableObjectIterator<T> iterator, T element1, T element2) {
		this.iterator = iterator;
		this.buffer = (T[]) new Object[2];
		this.buffer[0] = element1;
		this.buffer[1] = element2;
		try {
			this.hasNext = iterator.next(this.buffer[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return this.hasNext;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		T next = this.buffer[this.nextIndex];
		try {
			this.hasNext = this.iterator.next(this.buffer[this.nextIndex = (this.nextIndex + 1) % 2] = (T) new PactRecord());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return next;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
