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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * A queue that returns elements in LIFO order and simultaneously maintains set characteristics, i.e., elements
 * that are already in the queue may not be added another time.
 *
 * @param <E> The type of the elements in the queue.
 */
public class LifoSetQueue<E> extends AbstractQueue<E> implements Queue<E> {

	private final ArrayList<E> lifo = new ArrayList<E>();
	
	private final HashSet<E> set = new HashSet<E>();
	
	@Override
	public boolean offer(E e) {
		if (e == null) {
			throw new NullPointerException();
		}
		
		if (set.add(e)) {
			lifo.add(e);
		}
		
		return true;
	}

	@Override
	public E poll() {
		int size = lifo.size();
		if (size > 0) {
			E element = lifo.remove(size-1);
			set.remove(element);
			return element;
		} else {
			return null;
		}
	}

	@Override
	public E peek() {
		int size = lifo.size();
		if (size > 0) {
			return lifo.get(size-1);
		} else {
			return null;
		}
	}

	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {
			
			private int currentPos = lifo.size() - 1;
			private int posToRemove = -1;

			@Override
			public boolean hasNext() {
				return currentPos >= 0;
			}

			@Override
			public E next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				} else {
					posToRemove = currentPos;
					return lifo.get(currentPos--);
				}
			}

			@Override
			public void remove() {
				if (posToRemove == -1) {
					throw new NoSuchElementException();
				} else {
					E element = lifo.remove(posToRemove);
					set.remove(element);
				}
			}
		};
	}

	@Override
	public int size() {
		return lifo.size();
	}
}
