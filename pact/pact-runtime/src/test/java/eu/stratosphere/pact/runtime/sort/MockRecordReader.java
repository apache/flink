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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.types.Record;

/**
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public class MockRecordReader<T extends Record> implements Reader<T> {
	private static final Record SENTINEL = new Record() {
		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void read(DataInput in) throws IOException {
		}
	};

	private final BlockingQueue<Record> queue = new ArrayBlockingQueue<Record>(64, false);

	private T next;

	@Override
	public List<AbstractInputChannel<T>> getInputChannels() {
		return Collections.emptyList();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean hasNext() {
		if (next == null) {
			Record r = null;
			while (r == null) {
				try {
					r = queue.take();
				} catch (InterruptedException iex) {
				}
			}

			if (r == SENTINEL) {
				return false;
			} else {
				next = (T) r;
				return true;
			}
		} else {
			return true;
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public T next() throws IOException, InterruptedException {
		if (next != null) {
			T elem = next;
			next = null;
			return elem;
		}

		Record r = null;
		while (r == null) {
			try {
				r = queue.take();
			} catch (InterruptedException iex) {
			}
		}

		if (r == SENTINEL) {
			throw new NoSuchElementException();
		} else {
			return (T) r;
		}
	}

	public void emit(T element) throws InterruptedException {
		queue.put(element);
	}

	public void close() throws InterruptedException {
		queue.put(SENTINEL);
	}
}
