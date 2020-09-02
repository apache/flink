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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link SpillingThread.BufferWriter} which spills a result of applying a {@link GroupCombineFunction}.
 */
final class CombiningBufferWriter<R> implements SpillingThread.BufferWriter<R> {
	private static final Logger LOG = LoggerFactory.getLogger(CombiningBufferWriter.class);
	private final GroupCombineFunction<R, R> combineFunction;
	private final TypeSerializer<R> serializer;
	private final boolean objectReuseEnabled;

	CombiningBufferWriter(
			GroupCombineFunction<R, R> combineFunction,
			TypeSerializer<R> serializer,
			boolean objectReuseEnabled) {
		this.combineFunction = combineFunction;
		this.serializer = serializer;
		this.objectReuseEnabled = objectReuseEnabled;
	}

	@Override
	public void spillBuffer(
			CircularElement<R> element,
			ChannelWriterOutputView output,
			LargeRecordHandler<R> largeRecordHandler) throws IOException {
		// write sort-buffer to channel
		LOG.debug("Combining buffer {}.", element.getId());

		// set up the combining helpers
		final InMemorySorter<R> buffer = element.getBuffer();
		final CombineValueIterator<R> iter = new CombineValueIterator<>(
			buffer,
			this.serializer.createInstance(),
			this.objectReuseEnabled);
		final WriterCollector<R> collector = new WriterCollector<>(
			output,
			this.serializer);

		int i = 0;
		int stop = buffer.size() - 1;

		try {
			while (i < stop) {
				int seqStart = i;
				while (i < stop && 0 == buffer.compare(i, i + 1)) {
					i++;
				}

				if (i == seqStart) {
					// no duplicate key, no need to combine. simply copy
					buffer.writeToOutput(output, seqStart, 1);
				} else {
					// get the iterator over the values
					iter.set(seqStart, i);
					// call the combiner to combine
					combineFunction.combine(iter, collector);
				}
				i++;
			}
		} catch (Exception ex) {
			throw new IOException("An error occurred in the combiner user code.", ex);
		}

		// write the last pair, if it has not yet been included in the last iteration
		if (i == stop) {
			buffer.writeToOutput(output, stop, 1);
		}

		// done combining and writing out
		LOG.debug("Combined and spilled buffer {}.", element.getId());
	}
}
