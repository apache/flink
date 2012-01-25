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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * @author Erik Nijkamp
 * @author Stephan Ewen
 */
public class MockRecordReader implements MutableObjectIterator<PactRecord> {
	private final PactRecord SENTINEL = new PactRecord();

	private final BlockingQueue<PactRecord> queue;

	public MockRecordReader() {
		this.queue = new ArrayBlockingQueue<PactRecord>(32, false);
	}

	public MockRecordReader(int size) {
		this.queue = new ArrayBlockingQueue<PactRecord>(size, false);
	}

	@Override
	public boolean next(PactRecord target) {
		PactRecord r = null;
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
			return false;
		} else {
			r.copyTo(target);
			return true;
		}
	}

	public void emit(PactRecord element) throws InterruptedException {
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
