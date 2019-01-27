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

package org.apache.flink.table.sources.parquet;

import org.apache.hadoop.mapreduce.RecordReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * An adaptor from a Hadoop {@link RecordReader} to an {@link Iterator} over the values returned.
 * Note that this returns {@link Object}s instead of {@link org.apache.flink.types.Row} because we rely on erasure to pass
 * column batches by pretending they are rows.
 */
public class RecordReaderIterator<T> implements Iterator<T>, Closeable {

	private RecordReader<Void, T> reader;

	/**
	 * this means the function{@link #hasNext} is already invoked or not.
	 */
	private boolean alreadyHasNext = false;
	/**
	 * this field means the iteration has more elements or not.
	 */
	private boolean finished = false;

	public RecordReaderIterator(RecordReader<Void, T> reader) {
		this.reader = reader;
	}

	@Override
	public boolean hasNext() {
		try {
			if (!finished && !alreadyHasNext) {
				finished = !reader.nextKeyValue();
				if (finished) {
					// Close and release the reader here; close() will also be called when the task
					// completes, but for tasks that read from many files, it helps to release the
					// resources early.
					close();
				}
				alreadyHasNext = !finished;
			}
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
		return !finished;
	}

	@Override
	public T next() {
		try {
			if (!hasNext()) {
				throw new java.util.NoSuchElementException("End of stream");
			}
			alreadyHasNext = false;
			return reader.getCurrentValue();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			try {
				reader.close();
			} finally {
				reader = null;
			}
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove");
	}
}
