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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

public class DelayingIterator<T> implements MutableObjectIterator<T> {

	private final MutableObjectIterator<T> iterator;
	private final int delay;
	
	
	public DelayingIterator(MutableObjectIterator<T> iterator, int delay) {
		this.iterator = iterator;
		this.delay = delay;
	}
	
	@Override
	public T next(T reuse) throws IOException {
		try {
			Thread.sleep(delay);
		}
		catch (InterruptedException e) {
			// ignore, but restore interrupted state
			Thread.currentThread().interrupt();
		}
		return iterator.next(reuse);
	}

	@Override
	public T next() throws IOException {
		try {
			Thread.sleep(delay);
		}
		catch (InterruptedException e) {
			// ignore, but restore interrupted state
			Thread.currentThread().interrupt();
		}
		return iterator.next();
	}
}
