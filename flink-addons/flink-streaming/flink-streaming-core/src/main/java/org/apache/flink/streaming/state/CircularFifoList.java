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

package org.apache.flink.streaming.state;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
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

	public CircularFifoList() {
		this.queue = new LinkedList<T>();
		this.slideSizes = new LinkedList<Long>();
		this.counter = 0;
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
		Long firstSlideSize = slideSizes.remove();
		for (int i = 0; i < firstSlideSize; i++) {
			queue.remove();
		}
	}

	public Iterator<T> getIterator() {
		return queue.iterator();
	}

	@Override
	public String toString() {
		return queue.toString();
	}
}
