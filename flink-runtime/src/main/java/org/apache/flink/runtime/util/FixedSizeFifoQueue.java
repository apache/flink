/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Adding element to full queue removes its head and then adds new element. It's why size of this queue is fixed.
 * Example:
 * <pre>
 * {@code
 * Queue q = new FixedSizeFifoQueue<Long>(2);
 * q.add(1); // q = [1]
 * q.add(2); // q = [1, 2]
 * q.add(3); // q = [2, 3]
 * q.peek(); // 2
 * }
 * </pre>
 */
public class FixedSizeFifoQueue<E> extends ArrayBlockingQueue<E> {
	private int capacity;

	public FixedSizeFifoQueue(int capacity) {
		super(capacity);
		this.capacity = capacity;
	}

	@Override
	public boolean add(E e) {
		if (super.size() == this.capacity) {
			this.remove();
		}
		return super.add(e);
	}

	public boolean isFull() {
		return capacity == size();
	}
}
