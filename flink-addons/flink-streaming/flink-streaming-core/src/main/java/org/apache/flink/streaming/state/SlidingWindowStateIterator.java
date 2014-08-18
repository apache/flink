/**
 *
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
 *
 */

package org.apache.flink.streaming.state;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.streaming.api.invokable.operator.BatchIterator;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class SlidingWindowStateIterator<T> implements BatchIterator<T> {

	private CircularFifoBuffer buffer;
	private Iterator<Collection<StreamRecord<T>>> iterator;
	private Iterator<StreamRecord<T>> subIterator;
	private Iterator<StreamRecord<T>> streamRecordIterator;

	public SlidingWindowStateIterator(CircularFifoBuffer buffer) {
		this.buffer = buffer;
		this.streamRecordIterator = new StreamRecordIterator();
	}

	public boolean hasNext() {
		return subIterator.hasNext();
	}

	public T next() {
		T nextElement = subIterator.next().getObject();
		if (!subIterator.hasNext()) {
			if (iterator.hasNext()) {
				subIterator = iterator.next().iterator();
			}
		}
		return nextElement;
	}

	@Override
	public void remove() {
		throw new RuntimeException("Cannot use remove on reducing iterator.");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reset() {
		iterator = buffer.iterator();
		subIterator = iterator.next().iterator();
	}

	public Iterator<StreamRecord<T>> getStreamRecordIterator() {
		return this.streamRecordIterator;
	}

	private class StreamRecordIterator implements Iterator<StreamRecord<T>> {

		@Override
		public boolean hasNext() {
			return SlidingWindowStateIterator.this.hasNext();
		}

		@Override
		public StreamRecord<T> next() {
			StreamRecord<T> nextElement = subIterator.next();
			if (!subIterator.hasNext()) {
				if (iterator.hasNext()) {
					subIterator = iterator.next().iterator();
				}
			}
			return nextElement;
		}

		@Override
		public void remove() {
			SlidingWindowStateIterator.this.remove();
		}

	}
}
