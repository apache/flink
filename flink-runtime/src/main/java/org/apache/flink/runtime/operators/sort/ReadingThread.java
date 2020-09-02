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

import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The thread that consumes the input data and puts it into a buffer that will be sorted.
 */
final class ReadingThread<E> extends ThreadBase<E> {

	/** The input channels to read from. */
	private final MutableObjectIterator<E> reader;

	/** The object into which the thread reads the data from the input. */
	private final E readTarget;

	private final SorterInputGateway<E> sorterGateway;

	/**
	 * Creates a new reading thread.
	 *
	 * @param exceptionHandler The exception handler to call for all exceptions.
	 * @param reader The reader to pull the data from.
	 * @param dispatcher The queues used to pass buffers between the threads.
	 */
	ReadingThread(
			@Nullable ExceptionHandler<IOException> exceptionHandler,
			MutableObjectIterator<E> reader,
			StageMessageDispatcher<E> dispatcher,
			@Nullable LargeRecordHandler<E> largeRecordsHandler,
			@Nullable E readTarget,
			long startSpillingBytes) {
		super(exceptionHandler, "SortMerger Reading Thread", dispatcher);

		// members
		this.sorterGateway = new SorterInputGateway<>(dispatcher, largeRecordsHandler, startSpillingBytes);
		this.reader = checkNotNull(reader);
		this.readTarget = readTarget;
	}

	@Override
	public void go() throws IOException, InterruptedException {
		final MutableObjectIterator<E> reader = this.reader;

		E current = reader.next(readTarget);
		while (isRunning() && (current != null)) {
			sorterGateway.writeRecord(current);
			current = reader.next(current);
		}

		sorterGateway.finishReading();
	}
}
