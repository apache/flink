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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TraversableOnceException;

/**
 * This class wraps a {@link MutableObjectIterator} into a regular {@link Iterator}.
 * Internally, it uses two record instances which it uses alternating. That way,
 * whenever hasNext() returns (possibly with false), the previous obtained record is 
 * still valid and cannot have been overwritten internally.
 */
public class ReusingMutableToRegularIteratorWrapper<T> implements Iterator<T>, Iterable<T> {
	
	private final MutableObjectIterator<T> source;
	
	private T current, next;
	
	private boolean currentIsAvailable;
	
	private boolean iteratorAvailable = true;

	public ReusingMutableToRegularIteratorWrapper(MutableObjectIterator<T> source,
			TypeSerializer<T> serializer) {
		this.source = source;
		this.current = serializer.createInstance();
		this.next = serializer.createInstance();
	}

	@Override
	public boolean hasNext() {
		if (currentIsAvailable) {
			return true;
		} else {
			try {
				// we always use two records such that whenever hasNext() returns (possibly with false),
				// the previous record is always still valid.
				if ((next = source.next(next)) != null) {
					
					T tmp = current;
					current = next;
					next = tmp;
					
					currentIsAvailable = true;
					return true;
				} else {
					return false;
				}
			} catch (IOException ioex) {
				throw new RuntimeException("Error reading next record: " + ioex.getMessage(), ioex);
			}
		}
	}

	@Override
	public T next() {
		if (currentIsAvailable || hasNext()) {
			currentIsAvailable = false;
			return current;
		} else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> iterator() {
		if (iteratorAvailable) {
			iteratorAvailable = false;
			return this;
		}
		else {
			throw new TraversableOnceException();
		}
	}
}
