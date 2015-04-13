/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.state;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * A simple class, that manages a circular queue with sliding interval. If the
 * queue if full and a new element is added, the elements that belong to the
 * first sliding interval are removed.
 */
public class CircularFifoList<T> implements Serializable {
	private static final long serialVersionUID = 1L;

	private Queue<T> queue;
	private Queue<Long> slideSizes;
	private long counter;
	private Iterable<T> iterable;

	public CircularFifoList() {
		this.queue = new LinkedList<T>();
		this.slideSizes = new LinkedList<Long>();
		this.counter = 0;
		this.iterable = new ListIterable();
	}

	public void add(T element) {
		queue.add(element);
		counter++;
	}

	public void newSlide() {
		slideSizes.add(counter);
		counter = 0;
	}

	public void shiftWindow() {
		shiftWindow(1);
	}

	public void shiftWindow(int numberOfSlides) {

		if (numberOfSlides <= slideSizes.size()) {
			for (int i = 0; i < numberOfSlides; i++) {
				Long firstSlideSize = slideSizes.remove();

				for (int j = 0; j < firstSlideSize; j++) {
					queue.remove();
				}
			}
		} else {
			slideSizes.clear();
			queue.clear();
			counter = 0;
		}

	}
	
	@SuppressWarnings("unchecked")
	public List<T> getElements(){
		return (List<T>) queue;
	}

	public Iterator<T> getIterator() {
		return queue.iterator();
	}

	public Iterable<T> getIterable() {
		return iterable;
	}

	private class ListIterable implements Iterable<T>, Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<T> iterator() {
			return getIterator();
		}

	}
	
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public String toString() {
		return queue.toString();
	}

	
}
