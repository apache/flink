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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Queue;

/**
 * A queue that maintains set characteristics, i.e., elements
 * that are already in the queue may not be added another time.
 *
 * @param <E> The type of the elements in the queue.
 */
public class SetQueue<E> extends AbstractQueue<E> implements Queue<E> {

	private final LinkedHashSet<E> set = new LinkedHashSet<E>();
	
	@Override
	public boolean offer(E e) {
		if (e == null) {
			throw new NullPointerException();
		}
		
		// may, or may not, add the element.
		set.add(e);
		
		// we always return true, because the queue did not reject the element
		// due to capacity constraints
		return true;
	}

	@Override
	public E poll() {
		Iterator<E> iter = set.iterator();
		if (iter.hasNext()) {
			E next = iter.next();
			iter.remove();
			return next;
		} else {
			return null;
		}
	}

	@Override
	public E peek() {
		Iterator<E> iter = set.iterator();
		if (iter.hasNext()) {
			return iter.next();
		} else {
			return null;
		}
	}

	@Override
	public Iterator<E> iterator() {
		return set.iterator();
	}

	@Override
	public int size() {
		return set.size();
	}
	
	@Override
	public void clear() {
		set.clear();
	}
	
	@Override
	public boolean remove(Object o) {
		return set.remove(o);
	}
	
	@Override
	public boolean contains(Object o) {
		return set.contains(o);
	}
	
	@Override
	public boolean removeAll(Collection<?> c) {
		return set.removeAll(c);
	}
	
	@Override
	public boolean containsAll(Collection<?> c) {
		return set.containsAll(c);
	}
	
	@Override
	public boolean retainAll(Collection<?> c) {
		return set.retainAll(c);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return set.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj.getClass() == SetQueue.class) {
			return set.equals(((SetQueue<?>) obj).set);
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return set.toString();
	}
}
