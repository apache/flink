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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A merging policy based on outer sort-merge, and it also combining records with the same key during merging.
 */
public class CombiningRecordComparisonMerger<T> extends RecordComparisonMerger<T> {
	private final GroupCombineFunction<T, T> combineStub;


	public CombiningRecordComparisonMerger(GroupCombineFunction<T, T> combineStub,
			SortedDataFileFactory<T> sortedDataFileFactory,
			IOManager ioManager,
			TypeSerializer<T> typeSerializer,
			TypeComparator<T> typeComparator,
			int maxFanIn,
			boolean enableObjectReuse) {
		super(sortedDataFileFactory, ioManager, typeSerializer, typeComparator, maxFanIn, enableObjectReuse);
		this.combineStub = combineStub;
	}

	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The merging process
	 * uses the given read and write buffers. During the merging process, the combiner is used to reduce the
	 * number of values with identical key.
	 *
	 * @param channelIDs The IDs of the runs' channels.
	 * @param readBuffers The buffers for the readers that read the sorted runs.
	 * @param writeBuffers The buffers for the writer that writes the merged channel.
	 * @return The ID of the channel that describes the merged run.
	 */
	@Override
	protected SortedDataFile<T> mergeToNewFile(List<SortedDataFile<T>> channelIDs,
											   List<List<MemorySegment>> readBuffers,
											   List<MemorySegment> writeBuffers,
											   ChannelDeleteRegistry<T> channelDeleteRegistry) throws IOException {
		// the list with the readers, to be closed at shutdown
		final List<FileIOChannel> channelAccesses = new ArrayList<>(channelIDs.size());

		// the list with the target iterators
		final MergeIterator<T> mergeIterator = getMergingIteratorWithSegmentedMemory(channelIDs, readBuffers, channelAccesses, null, channelDeleteRegistry);

		// create a new channel writer
		SortedDataFile<T> output = sortedDataFileFactory.createFile(writeBuffers);
		channelDeleteRegistry.registerChannelToBeDelete(output.getChannelID());
		channelDeleteRegistry.registerOpenChannel(output.getWriteChannel());

		final WriterCollector<T> collector = new WriterCollector<T>(output);
		final GroupCombineFunction<T, T> combineStub = this.combineStub;

		// combine and write to disk
		try {
			if (objectReuseEnabled) {
				final ReusingKeyGroupedIterator<T> groupedIter = new ReusingKeyGroupedIterator<>(
					mergeIterator, this.typeSerializer, this.typeComparator);
				while (groupedIter.nextKey()) {
					combineStub.combine(groupedIter.getValues(), collector);
				}
			} else {
				final NonReusingKeyGroupedIterator<T> groupedIter = new NonReusingKeyGroupedIterator<>(
					mergeIterator, this.typeComparator);
				while (groupedIter.nextKey()) {
					combineStub.combine(groupedIter.getValues(), collector);
				}
			}
		}
		catch (Exception e) {
			throw new IOException("An error occurred in the combiner user code.");
		}

		output.finishWriting();  //IS VERY IMPORTANT!!!!

		// register merged result to be removed at shutdown
		channelDeleteRegistry.unregisterOpenChannel(output.getWriteChannel());

		// remove the merged channel readers from the clear-at-shutdown list
		for (int i = 0; i < channelAccesses.size(); i++) {
			FileIOChannel access = channelAccesses.get(i);
			ioManager.deleteChannel(access.getChannelID());
			channelDeleteRegistry.unregisterOpenChannel(access);
		}

		return output;
	}

}
