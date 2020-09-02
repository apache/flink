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
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;

import java.io.IOException;

/**
 * A {@link SpillingThread.RecordsMerger} that combines records when merging.
 *
 * @see DefaultRecordsMerger
 */
final class CombiningRecordsMerger<R> implements SpillingThread.RecordsMerger<R> {

	private final GroupCombineFunction<R, R> combineFunction;
	private final TypeSerializer<R> serializer;
	private final TypeComparator<R> comparator;
	private final boolean objectReuseEnabled;

	CombiningRecordsMerger(
			GroupCombineFunction<R, R> combineFunction,
			TypeSerializer<R> serializer,
			TypeComparator<R> comparator,
			boolean objectReuseEnabled) {
		this.combineFunction = combineFunction;
		this.serializer = serializer;
		this.comparator = comparator;
		this.objectReuseEnabled = objectReuseEnabled;
	}

	@Override
	public void mergeRecords(
		MergeIterator<R> mergeIterator,
		ChannelWriterOutputView output) throws IOException {
		final WriterCollector<R> collector = new WriterCollector<>(
			output,
			this.serializer);

		// combine and write to disk
		try {
			if (objectReuseEnabled) {
				final ReusingKeyGroupedIterator<R> groupedIter = new ReusingKeyGroupedIterator<>(
					mergeIterator, this.serializer, this.comparator);
				while (groupedIter.nextKey()) {
					combineFunction.combine(groupedIter.getValues(), collector);
				}
			} else {
				final NonReusingKeyGroupedIterator<R> groupedIter = new NonReusingKeyGroupedIterator<>(
					mergeIterator, this.comparator);
				while (groupedIter.nextKey()) {
					combineFunction.combine(groupedIter.getValues(), collector);
				}
			}
		} catch (Exception e) {
			throw new IOException("An error occurred in the combiner user code.");
		}
	}
}
