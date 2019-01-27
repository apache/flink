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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelBackendMutableObjectIterator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A merging policy who merge files by running outer sort-merge.
 */
public class RecordComparisonMerger<T> implements SortedDataFileMerger<T> {
	private static final Logger LOG = LoggerFactory.getLogger(RecordComparisonMerger.class);

	private List<SortedDataFile<T>> sortedDataFiles;

	protected final SortedDataFileFactory<T> sortedDataFileFactory;

	protected final IOManager ioManager;

	protected final TypeSerializer<T> typeSerializer;
	protected final TypeComparator<T> typeComparator;

	private final int maxFileHandlesPerMerge;
	protected final boolean objectReuseEnabled;

	public RecordComparisonMerger(SortedDataFileFactory<T> sortedDataFileFactory,
								  IOManager ioManager,
								  TypeSerializer<T> typeSerializer,
								  TypeComparator<T> typeComparator,
								  int maxFileHandlesPerMerge,
								  boolean objectReuseEnabled) {
		this.sortedDataFileFactory = sortedDataFileFactory;
		this.ioManager = ioManager;
		this.typeSerializer = typeSerializer;
		this.typeComparator = typeComparator;
		this.maxFileHandlesPerMerge = maxFileHandlesPerMerge;
		this.objectReuseEnabled = objectReuseEnabled;
		this.sortedDataFiles = new ArrayList<>();
	}

	private void merge(List<MemorySegment> writeMemory,
							   List<MemorySegment> mergeReadMemory,
							   ChannelDeleteRegistry<T> channelDeleteRegistry,
							   AtomicBoolean aliveFlag) throws IOException {
		int maxFanIn = Math.min(maxFileHandlesPerMerge, mergeReadMemory.size());

		while (aliveFlag.get() && sortedDataFiles.size() > maxFanIn) {
			sortedDataFiles = mergeChannelList(
				sortedDataFiles, mergeReadMemory, writeMemory, channelDeleteRegistry, maxFanIn);
		}
	}

	@Override
	public MutableObjectIterator<T> getMergingIterator(List<SortedDataFile<T>> channels,
													   List<MemorySegment> mergeReadMemory,
													   MutableObjectIterator<T> largeRecords,
													   ChannelDeleteRegistry<T> channelDeleteRegistry) throws IOException {
		List<List<MemorySegment>> segmentedReadMemory = distributeReadMemory(mergeReadMemory, channels.size());
		return getMergingIteratorWithSegmentedMemory(channels, segmentedReadMemory, null, largeRecords, channelDeleteRegistry);
	}

	@Override
	public void notifyNewSortedDataFile(SortedDataFile<T> sortedDataFile,
										   List<MemorySegment> writeMemory,
										   List<MemorySegment> mergeReadMemory,
										   ChannelDeleteRegistry<T> channelDeleteRegistry,
										   AtomicBoolean aliveFlag) throws IOException {
		sortedDataFiles.add(sortedDataFile);
	}

	@Override
	public List<SortedDataFile<T>> finishMerging(List<MemorySegment> writeMemory,
												 List<MemorySegment> mergeReadMemory,
												 ChannelDeleteRegistry<T> channelDeleteRegistry,
												 AtomicBoolean aliveFlag) throws IOException {
		merge(writeMemory, mergeReadMemory, channelDeleteRegistry, aliveFlag);
		return sortedDataFiles;
	}

	/**
	 * Returns an iterator that iterates over the merged result from all given channels.
	 *
	 * @param files the channels that are to be merged and returned.
	 * @param inputSegments the buffers to be used for reading. The list contains for each channel one
	 *                      list of input segments. The size of the <code>inputSegments</code> list must be equal to
	 *                      that of the <code>files</code> list.
	 * @param channelAccessed the list to store the channels opened during merging, if needed.
	 * @param largeRecords the iterator of large records, if present.
	 * @param channelDeleteRegistry the registry to manage files to be close and delete.
	 *
	 * @return an iterator over the merged records of the input channels.
	 * @throws IOException thrown if the readers encounter an I/O problem.
	 */
	protected final MergeIterator<T> getMergingIteratorWithSegmentedMemory(List<SortedDataFile<T>> files,
																			List<List<MemorySegment>> inputSegments,
																			List<FileIOChannel> channelAccessed,
																			MutableObjectIterator<T> largeRecords,
																			ChannelDeleteRegistry<T> channelDeleteRegistry) throws IOException {
		// create one iterator per channel id
		if (LOG.isDebugEnabled()) {
			LOG.debug("Performing merge of " + files.size() + " sorted streams.");
		}

		final List<MutableObjectIterator<T>> iterators = new ArrayList<>(files.size() + 1);

		for (int i = 0; i < files.size(); i++) {
			final List<MemorySegment> segsForChannel = inputSegments.get(i);

			ChannelBackendMutableObjectIterator<T> channelBackendMutableObjectIterator = files.get(i).createReader(segsForChannel);

			if (channelAccessed != null) {
				channelAccessed.add(channelBackendMutableObjectIterator.getReaderChannel());
			}

			channelDeleteRegistry.registerOpenChannel(channelBackendMutableObjectIterator.getReaderChannel());
			channelDeleteRegistry.registerChannelToBeDelete(channelBackendMutableObjectIterator.getReaderChannel().getChannelID());

			iterators.add(channelBackendMutableObjectIterator);
		}

		if (largeRecords != null) {
			iterators.add(largeRecords);
		}

		return new MergeIterator<T>(iterators, this.typeComparator);
	}

	/**
	 * Merges the given sorted runs to a smaller number of sorted runs.
	 *
	 * @param files The IDs of the sorted runs that need to be merged.
	 * @param allReadBuffers The buffers to be used by the readers.
	 * @param writeBuffers The buffers to be used by the writers.
	 * @return A list of the IDs of the merged channels.
	 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
	 */
	protected final List<SortedDataFile<T>> mergeChannelList(List<SortedDataFile<T>> files,
															 List<MemorySegment> allReadBuffers,
															 List<MemorySegment> writeBuffers,
															 ChannelDeleteRegistry<T> channelDeleteRegistry,
															 int maxFanIn) throws IOException {
		// A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1 rounds where every merge
		// is a full merge with maxFanIn input channels. A partial round includes merges with fewer than maxFanIn
		// inputs. It is most efficient to perform the partial round first.
		final double scale = Math.ceil(Math.log(files.size()) / Math.log(maxFanIn)) - 1;

		final int numStart = files.size();
		final int numEnd = (int) Math.pow(maxFanIn, scale);

		final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (maxFanIn - 1));

		final int numNotMerged = numEnd - numMerges;
		final int numToMerge = numStart - numNotMerged;

		// unmerged channel IDs are copied directly to the result list
		final List<SortedDataFile<T>> mergedFiles = new ArrayList<>(numEnd);
		mergedFiles.addAll(files.subList(0, numNotMerged));

		final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

		// allocate the memory for the merging step
		final List<List<MemorySegment>> segmentedFileChannels = distributeReadMemory(allReadBuffers, channelsToMergePerStep);
		final List<SortedDataFile<T>> channelsToMergeThisStep = new ArrayList<>(channelsToMergePerStep);
		int channelNum = numNotMerged;
		while (channelNum < files.size()) {
			channelsToMergeThisStep.clear();

			for (int i = 0; i < channelsToMergePerStep && channelNum < files.size(); i++, channelNum++) {
				channelsToMergeThisStep.add(files.get(channelNum));
			}

			mergedFiles.add(mergeToNewFile(channelsToMergeThisStep, segmentedFileChannels, writeBuffers, channelDeleteRegistry));
		}

		return mergedFiles;
	}

	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The merging process
	 * uses the given read and write buffers.
	 *
	 * @param files The IDs of the runs' channels.
	 * @param readBuffers The buffers for the readers that read the sorted runs.
	 * @param writeBuffers The buffers for the writer that writes the merged channel.
	 * @return The ID and number of blocks of the channel that describes the merged run.
	 */
	protected SortedDataFile<T> mergeToNewFile(List<SortedDataFile<T>> files,
											   List<List<MemorySegment>> readBuffers,
											   List<MemorySegment> writeBuffers,
											   ChannelDeleteRegistry<T> channelDeleteRegistry) throws IOException {
		// the list with the readers, to be closed at shutdown
		final List<FileIOChannel> channelAccesses = new ArrayList<>(files.size());

		// the list with the target iterators
		final MergeIterator<T> mergeIterator = getMergingIteratorWithSegmentedMemory(files, readBuffers,
			channelAccesses, null, channelDeleteRegistry);

		final SortedDataFile<T> writer = sortedDataFileFactory.createFile(writeBuffers);
		channelDeleteRegistry.registerChannelToBeDelete(writer.getChannelID());
		channelDeleteRegistry.registerOpenChannel(writer.getWriteChannel());

		// read the merged stream and write the data back
		if (objectReuseEnabled) {
			T rec = typeSerializer.createInstance();
			while ((rec = mergeIterator.next(rec)) != null) {
				writer.writeRecord(rec);
			}
		} else {
			T rec;
			while ((rec = mergeIterator.next()) != null) {
				writer.writeRecord(rec);
			}
		}

		writer.finishWriting();

		// unregister merged result to be removed at shutdown
		channelDeleteRegistry.unregisterOpenChannel(writer.getWriteChannel());

		// remove the merged channel readers from the clear-at-shutdown list
		for (int i = 0; i < channelAccesses.size(); i++) {
			FileIOChannel access = channelAccesses.get(i);
			access.closeAndDelete();
			channelDeleteRegistry.unregisterOpenChannel(access);
		}

		return writer;
	}

	/**
	 * Divides the given collection of memory buffers among {@code numChannels} sublists.
	 *
	 * @param memory A list containing the memory buffers to be distributed. The buffers are not
	 *               removed from this list.
	 * @param numChannels The number of channels for which to allocate buffers. Must not be zero.
	 */
	protected final List<List<MemorySegment>> distributeReadMemory(List<MemorySegment> memory, int numChannels) {
		List<List<MemorySegment>> target = new ArrayList<>(numChannels);

		// determine the memory to use per channel and the number of buffers
		final int numBuffers = memory.size();
		final int buffersPerChannelLowerBound = numBuffers / numChannels;
		final int numChannelsWithOneMore = numBuffers % numChannels;

		final Iterator<MemorySegment> segments = memory.iterator();

		// collect memory for the channels that get one segment more
		for (int i = 0; i < numChannels; i++) {
			int toAssign = (i < numChannelsWithOneMore ? buffersPerChannelLowerBound + 1 : buffersPerChannelLowerBound);
			final ArrayList<MemorySegment> segs = new ArrayList<>(toAssign);
			target.add(segs);

			for (int j = 0; j < toAssign; ++j) {
				segs.add(segments.next());
			}
		}

		return target;
	}
}
