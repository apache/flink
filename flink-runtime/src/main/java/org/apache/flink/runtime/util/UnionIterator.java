/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.util.TraversableOnceException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class UnionIterator<T> implements Iterator<T>, Iterable<T> {
	
	private Iterator<T> currentIterator;
	
	private ArrayList<List<T>> furtherLists = new ArrayList<>();
	
	private int nextList;
	
	private boolean iteratorAvailable = true;

	// ------------------------------------------------------------------------
	
	public void clear() {
		currentIterator = null;
		furtherLists.clear();
		nextList = 0;
		iteratorAvailable = true;
	}
	
	public void addList(List<T> list) {
		if (currentIterator == null) {
			currentIterator = list.iterator();
		}
		else {
			furtherLists.add(list);
		}
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public Iterator<T> iterator() {
		if (iteratorAvailable) {
			iteratorAvailable = false;
			return this;
		} else {
			throw new TraversableOnceException();
		}
	}

	@Override
	public boolean hasNext() {
		while (currentIterator != null) {
			if (currentIterator.hasNext()) {
				return true;
			}
			else if (nextList < furtherLists.size()) {
				currentIterator = furtherLists.get(nextList).iterator();
				nextList++;
			}
			else {
				currentIterator = null;
			}
		}
		
		return false;
	}

	@Override
	public T next() {
		if (hasNext()) {
			return currentIterator.next();
		}
		else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
