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


package org.apache.flink.runtime.operators.sort;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;

/**
 */
public class MockRecordReader implements MutableObjectIterator<Record> {
	private final Record SENTINEL = new Record();

	private final BlockingQueue<Record> queue;

	public MockRecordReader() {
		this.queue = new ArrayBlockingQueue<Record>(32, false);
	}

	public MockRecordReader(int size) {
		this.queue = new ArrayBlockingQueue<Record>(size, false);
	}

	@Override
	public Record next(Record reuse) {
		Record r = null;
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
				queue.put(r);
			} catch (InterruptedException e) {
				throw new RuntimeException("Reader was interrupted.");
			}
			return null;
		} else {
			r.copyTo(reuse);
			return reuse;
		}
	}

	@Override
	public Record next() {
		Record r = null;
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
				queue.put(r);
			} catch (InterruptedException e) {
				throw new RuntimeException("Reader was interrupted.");
			}
			return null;
		} else {
			Record result = new Record(r.getNumFields());
			r.copyTo(result);
			return result;
		}
	}

	public void emit(Record element) throws InterruptedException {
		queue.put(element.createCopy());
	}

	public void close() {
		try {
			queue.put(SENTINEL);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
