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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The specific delegate implementation for the multiple outputs case.
 */
public class MultipleRecordWriters<T extends IOReadableWritable> implements RecordWriterDelegate<T> {

	/** The real record writer instances for this delegate. */
	private final List<RecordWriter<T>> recordWriters;

	/**
	 * Maintains the respective record writer futures to avoid allocating new arrays every time
	 * in {@link #getAvailableFuture()}.
	 */
	private final CompletableFuture<?>[] futures;

	public MultipleRecordWriters(List<RecordWriter<T>> recordWriters) {
		this.recordWriters = checkNotNull(recordWriters);
		this.futures = new CompletableFuture[recordWriters.size()];
	}

	@Override
	public void broadcastEvent(AbstractEvent event) throws IOException {
		IOException exception = null;
		for (RecordWriter recordWriter : recordWriters) {
			try {
				recordWriter.broadcastEvent(event);
			} catch (IOException e) {
				exception = ExceptionUtils.firstOrSuppressed(
					new IOException("Could not send event to downstream tasks.", e), exception);
			}
		}

		if (exception != null) {
			throw exception;
		}
	}

	@Override
	public RecordWriter<T> getRecordWriter(int outputIndex) {
		return recordWriters.get(outputIndex);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		for (int i = 0; i < futures.length; i++) {
			futures[i] = recordWriters.get(i).getAvailableFuture();
		}
		return CompletableFuture.allOf(futures);
	}

	@Override
	public boolean isAvailable() {
		for (RecordWriter recordWriter : recordWriters) {
			if (!recordWriter.isAvailable()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void close() {
		for (RecordWriter recordWriter : recordWriters) {
			recordWriter.close();
		}
	}
}
