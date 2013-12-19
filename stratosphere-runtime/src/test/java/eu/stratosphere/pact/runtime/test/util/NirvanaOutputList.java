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

package eu.stratosphere.pact.runtime.test.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.types.Record;

public class NirvanaOutputList implements List<Record> {

	@Override
	public boolean add(Record arg0) {
		return true;
	}

	@Override
	public void add(int arg0, Record arg1) {
	}

	@Override
	public boolean addAll(Collection<? extends Record> arg0) {
		return true;
	}

	@Override
	public boolean addAll(int arg0, Collection<? extends Record> arg1) {
		return true;
	}

	@Override
	public void clear() {
	}

	@Override
	public boolean contains(Object arg0) {
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		return false;
	}

	@Override
	public Record get(int arg0) {
		return null;
	}

	@Override
	public int indexOf(Object arg0) {
		return -1;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public Iterator<Record> iterator() {
		
		return new Iterator<Record>() {

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public Record next() {
				return null;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	@Override
	public int lastIndexOf(Object arg0) {
		return -1;
	}

	@Override
	public ListIterator<Record> listIterator() {
		return null;
	}

	@Override
	public ListIterator<Record> listIterator(int arg0) {
		return null;
	}

	@Override
	public boolean remove(Object arg0) {
		return true;
	}

	@Override
	public Record remove(int arg0) {
		return null;
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		return true;
	}

	@Override
	public Record set(int arg0, Record arg1) {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public List<Record> subList(int arg0, int arg1) {
		return new NirvanaOutputList();
	}

	@Override
	public Object[] toArray() {
		return new Object[0];
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		return null;
	}

	
}
