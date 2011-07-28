/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.runtime.util.ReadingIterator;

/**
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public class MockRecordReader<T extends Record> implements ReadingIterator<T>
{
	private static final Record SENTINEL = new Record() {
		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void read(DataInput in) throws IOException {
		}
	};

	private final BlockingQueue<T> queue = new ArrayBlockingQueue<T>(64, false);
	

	@Override
	public T next(T target)
	{
		T r = null;
		while (r == null) {
			try {
				r = queue.take();
			} catch (InterruptedException iex) {
				throw new RuntimeException("Reader was interrupted.");
			}
		}

		if (r == SENTINEL) {
			// put the sentinel back, to ensure that repeated calls do not block
			try {
				queue.put((T)r);
			} catch (InterruptedException e) {
				throw new RuntimeException("Reader was interrupted.");
			}
			return null;
		} else {
			return r;
		}
	}

	public void emit(T element) throws InterruptedException {
		queue.put(element);
	}
	
	@SuppressWarnings("unchecked")
	public void close() {
		try {
			queue.put((T) SENTINEL);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
