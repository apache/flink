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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.operators.sort.ChannelDeleteRegistry;
import org.apache.flink.runtime.operators.sort.DefaultFileMergePolicy;
import org.apache.flink.runtime.operators.sort.PartialOrderPriorityQueue;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.operators.sort.DataFileInfo;
import org.apache.flink.runtime.operators.sort.SortedDataFileMerger;
import org.apache.flink.runtime.operators.sort.MergePolicy;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A merger who simply concats the part of the same partition together.
 */
public class ConcatPartitionedFileMerger<T> implements SortedDataFileMerger<Tuple2<Integer, T>> {
	private static final Logger LOG = LoggerFactory.getLogger(ConcatPartitionedFileMerger.class);

	private final int numberOfSubpartitions;
	private final String partitionDataRootPath;

	private final IOManager ioManager;

	private final MergePolicy<SortedDataFile<Tuple2<Integer, T>>> mergePolicy;

	/** The merge file index starts from the max value to avoid using the same file id with spilled ones. */
	private int mergeFileIndex = Integer.MAX_VALUE;

	ConcatPartitionedFileMerger(int numberOfSubpartitions,
								String partitionDataRootPath,
								int mergeFactor,
								boolean enableAsyncMerging,
								boolean mergeToOneFile,
								IOManager ioManager) {
		Preconditions.checkArgument(numberOfSubpartitions > 0, "Illegal subpartition number: " + numberOfSubpartitions);
		Preconditions.checkArgument(mergeFactor > 0, "Illegal merge factor: " + mergeFactor);

		this.numberOfSubpartitions = numberOfSubpartitions;
		this.partitionDataRootPath = Preconditions.checkNotNull(partitionDataRootPath);

		this.ioManager = Preconditions.checkNotNull(ioManager);

		this.mergePolicy = new DefaultFileMergePolicy<>(mergeFactor, enableAsyncMerging, mergeToOneFile);
	}

	@Override
	public MutableObjectIterator<Tuple2<Integer, T>> getMergingIterator(List<SortedDataFile<Tuple2<Integer, T>>> sortedDataFiles,
																		List<MemorySegment> mergeReadMemory,
																		MutableObjectIterator<Tuple2<Integer, T>> largeRecords,
																		ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry) throws IOException {
		//TODO: will be implemented later.
		return new MutableObjectIterator<Tuple2<Integer, T>>() {
			@Override
			public Tuple2<Integer, T> next(Tuple2<Integer, T> reuse) throws IOException {
				return null;
			}

			@Override
			public Tuple2<Integer, T> next() throws IOException {
				return null;
			}
		};
	}

	@Override
	public void notifyNewSortedDataFile(SortedDataFile<Tuple2<Integer, T>> sortedDataFile,
										List<MemorySegment> writeMemory,
										List<MemorySegment> mergeReadMemory,
										ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry,
										AtomicBoolean aliveFlag) throws IOException {
		if (!(sortedDataFile instanceof PartitionedBufferSortedDataFile)) {
			throw new IllegalArgumentException("Only PartitionedBufferSortedDataFile is supported: " + sortedDataFile.getClass().getName());
		}

		DataFileInfo<SortedDataFile<Tuple2<Integer, T>>> dataFileInfo = new DataFileInfo<>(
			sortedDataFile.getBytesWritten(), 0, numberOfSubpartitions, sortedDataFile);
		mergePolicy.addNewCandidate(dataFileInfo);

		mergeIfPossible(mergeReadMemory, channelDeleteRegistry, aliveFlag);
	}

	@Override
	public List<SortedDataFile<Tuple2<Integer, T>>> finishMerging(List<MemorySegment> writeMemory,
																  List<MemorySegment> mergeReadMemory,
																  ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry,
																  AtomicBoolean aliveFlag) throws IOException {
		mergePolicy.startFinalMerge();
		mergeIfPossible(mergeReadMemory, channelDeleteRegistry, aliveFlag);

		return mergePolicy.getFinalMergeResult();
	}

	private void mergeIfPossible(List<MemorySegment> mergeReadMemory,
								 ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry,
								 AtomicBoolean aliveFlag) throws IOException {
		// select merge candidates
		List<DataFileInfo<SortedDataFile<Tuple2<Integer, T>>>> mergeCandidates = mergePolicy.selectMergeCandidates(mergeReadMemory.size());
		while (mergeCandidates != null && aliveFlag.get()) {
			int maxMergeRound = 0;
			LinkedList<PartitionedSortedDataFile<T>> toBeMerged = new LinkedList<>();

			for (DataFileInfo<SortedDataFile<Tuple2<Integer, T>>> mergeCandidate: mergeCandidates) {
				maxMergeRound = Math.max(maxMergeRound, mergeCandidate.getMergeRound());
				PartitionedSortedDataFile<T> partitionedSortedDataFile = (PartitionedSortedDataFile<T>) mergeCandidate.getDataFile();
				toBeMerged.add(partitionedSortedDataFile);
			}

			LOG.info("Start merging {} files to one file.", toBeMerged.size());

			try {
				// merge the candidates to one file
				SortedDataFile<Tuple2<Integer, T>> mergedFile = mergeToOutput(
					toBeMerged, mergeReadMemory, channelDeleteRegistry, mergeFileIndex--);
				DataFileInfo<SortedDataFile<Tuple2<Integer, T>>> mergedFileInfo = new DataFileInfo<>(
					mergedFile.getBytesWritten(), maxMergeRound + 1, numberOfSubpartitions, mergedFile);
				// notify new file
				mergePolicy.addNewCandidate(mergedFileInfo);
			} catch (InterruptedException e) {
				throw new RuntimeException("Merge was interrupted.", e);
			}
			// select new candidates
			mergeCandidates = mergePolicy.selectMergeCandidates(mergeReadMemory.size());
		}
	}

	private ConcatPartitionedBufferSortedDataFile<T>  mergeToOutput(List<PartitionedSortedDataFile<T>> toBeMerged,
																	List<MemorySegment> mergeReadMemory,
																	ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry,
																	int fileId) throws IOException, InterruptedException {
		// Create merged file writer.
		final FileIOChannel.ID channel = ioManager.createChannel(new File(ExternalBlockShuffleUtils.generateMergePath(partitionDataRootPath, fileId)));
		ConcatPartitionedBufferSortedDataFile<T> writer = new ConcatPartitionedBufferSortedDataFile<T>(
			numberOfSubpartitions, channel, fileId, ioManager);
		channelDeleteRegistry.registerChannelToBeDelete(channel);
		channelDeleteRegistry.registerOpenChannel(writer.getWriteChannel());

		// Create file readers.
		final List<List<MemorySegment>> segments = Lists.partition(mergeReadMemory, mergeReadMemory.size() / toBeMerged.size());

		final PartialOrderPriorityQueue<PartitionIndexStream> heap = new PartialOrderPriorityQueue<>(
			new PartitionIndexStreamComparator(), toBeMerged.size());

		Set<AsynchronousPartitionedStreamFileReaderDelegate> allReaders = new HashSet<>();
		for (int i = 0; i < toBeMerged.size(); ++i) {
			AsynchronousPartitionedStreamFileReaderDelegate readerDelegate =
				new AsynchronousPartitionedStreamFileReaderDelegate(
					ioManager, toBeMerged.get(i).getChannelID(), segments.get(i),
					toBeMerged.get(i).getPartitionIndexList());
			heap.add(new PartitionIndexStream(readerDelegate, toBeMerged.get(i).getPartitionIndexList()));
			// will be used when closing read files
			allReaders.add(readerDelegate);
			// register the opened channel to be closed when error happens
			channelDeleteRegistry.registerOpenChannel(readerDelegate.getReader());
		}

		while (heap.size() > 0) {
			final PartitionIndexStream headStream = heap.peek();
			final PartitionIndex partitionIndex = headStream.getCurrentPartitionIndex();

			if (!headStream.advance()) {
				heap.poll();
			} else {
				heap.adjustTop();
			}

			// now read the specific counts of buffers
			int readLength = 0;
			while (readLength < partitionIndex.getLength()) {
				Buffer buffer = headStream.getReader().getNextBufferBlocking();
				readLength += buffer.getSize();
				writer.writeBuffer(partitionIndex.getPartition(), buffer);
			}
			assert readLength == partitionIndex.getLength();
		}

		writer.finishWriting();
		channelDeleteRegistry.unregisterOpenChannel(writer.getWriteChannel());

		clearMerged(channelDeleteRegistry, allReaders);
		return writer;
	}

	private void clearMerged(ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry,
							 Set<AsynchronousPartitionedStreamFileReaderDelegate> allReaders) throws IOException {

		// close the reader and delete the underlying file for already merged channels
		for (AsynchronousPartitionedStreamFileReaderDelegate reader : allReaders) {
			// close the file reader
			reader.close();
			channelDeleteRegistry.unregisterOpenChannel(reader.getReader());
			// delete the file
			reader.getReader().deleteChannel();
			channelDeleteRegistry.unregisterChannelToBeDelete(reader.getReader().getChannelID());
		}
		allReaders.clear();
	}

	private static final class PartitionIndexStream {
		private final AsynchronousPartitionedStreamFileReaderDelegate reader;
		private final List<PartitionIndex> partitionIndices;
		private int offset;

		public PartitionIndexStream(AsynchronousPartitionedStreamFileReaderDelegate reader,
									List<PartitionIndex> partitionIndices) {
			this.reader = reader;
			this.partitionIndices = partitionIndices;
			this.offset = 0;
		}

		public PartitionIndex getCurrentPartitionIndex() {
			return partitionIndices.get(offset);
		}

		public AsynchronousPartitionedStreamFileReaderDelegate getReader() {
			return reader;
		}

		public boolean advance() {
			if (offset < partitionIndices.size() - 1) {
				offset++;
				return true;
			}

			return false;
		}

		@Override
		public String toString() {
			return "PartitionIndexStream{" +
				"partitionIndices=" + partitionIndices.size() +
				", offset=" + offset +
				'}';
		}
	}

	private static final class PartitionIndexStreamComparator implements Comparator<PartitionIndexStream> {
		@Override
		public int compare(PartitionIndexStream first, PartitionIndexStream second) {
			int firstPartition = first.getCurrentPartitionIndex().getPartition();
			int secondPartition = second.getCurrentPartitionIndex().getPartition();
			if (firstPartition != secondPartition) {
				return firstPartition < secondPartition ? -1 : 1;
			}

			long firstStart = first.getCurrentPartitionIndex().getStartOffset();
			long secondStart = second.getCurrentPartitionIndex().getStartOffset();
			return Long.compare(firstStart, secondStart);
		}
	}
}
