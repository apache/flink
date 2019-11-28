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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.runtime.compression.BlockCompressionFactory;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Spilled files Merger of {@link BinaryExternalSorter}.
 * It merges {@link #maxFanIn} spilled files at most once.
 *
 * @param <Entry> Type of Entry to Merge sort.
 */
public abstract class AbstractBinaryExternalMerger<Entry> implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryExternalMerger.class);

	private volatile boolean closed;

	private final int maxFanIn;
	private final SpillChannelManager channelManager;
	private final boolean compressionEnable;
	private final BlockCompressionFactory compressionCodecFactory;
	private final int compressionBlockSize;

	protected final int pageSize;
	protected final IOManager ioManager;

	public AbstractBinaryExternalMerger(
			IOManager ioManager,
			int pageSize,
			int maxFanIn,
			SpillChannelManager channelManager,
			boolean compressionEnable,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize) {
		this.ioManager = ioManager;
		this.pageSize = pageSize;
		this.maxFanIn = maxFanIn;
		this.channelManager = channelManager;
		this.compressionEnable = compressionEnable;
		this.compressionCodecFactory = compressionCodecFactory;
		this.compressionBlockSize = compressionBlockSize;
	}

	@Override
	public void close() {
		this.closed = true;
	}

	/**
	 * Returns an iterator that iterates over the merged result from all given channels.
	 *
	 * @param channelIDs    The channels that are to be merged and returned.
	 * @return An iterator over the merged records of the input channels.
	 * @throws IOException Thrown, if the readers encounter an I/O problem.
	 */
	public BinaryMergeIterator<Entry> getMergingIterator(
			List<ChannelWithMeta> channelIDs,
			List<FileIOChannel> openChannels)
			throws IOException {
		// create one iterator per channel id
		if (LOG.isDebugEnabled()) {
			LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
		}

		final List<MutableObjectIterator<Entry>> iterators = new ArrayList<>(channelIDs.size() + 1);

		for (ChannelWithMeta channel : channelIDs) {
			AbstractChannelReaderInputView view = FileChannelUtil.createInputView(
					ioManager, channel, openChannels, compressionEnable, compressionCodecFactory,
					compressionBlockSize, pageSize);
			iterators.add(channelReaderInputViewIterator(view));
		}

		return new BinaryMergeIterator<>(
				iterators, mergeReusedEntries(channelIDs.size()), mergeComparator());
	}

	/**
	 * Merges the given sorted runs to a smaller number of sorted runs.
	 *
	 * @param channelIDs     The IDs of the sorted runs that need to be merged.
	 * @return A list of the IDs of the merged channels.
	 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
	 */
	public List<ChannelWithMeta> mergeChannelList(List<ChannelWithMeta> channelIDs) throws IOException {
		// A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1 rounds where every merge
		// is a full merge with maxFanIn input channels. A partial round includes merges with fewer than maxFanIn
		// inputs. It is most efficient to perform the partial round first.
		final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(maxFanIn)) - 1;

		final int numStart = channelIDs.size();
		final int numEnd = (int) Math.pow(maxFanIn, scale);

		final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (maxFanIn - 1));

		final int numNotMerged = numEnd - numMerges;
		final int numToMerge = numStart - numNotMerged;

		// unmerged channel IDs are copied directly to the result list
		final List<ChannelWithMeta> mergedChannelIDs = new ArrayList<>(numEnd);
		mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

		final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

		final List<ChannelWithMeta> channelsToMergeThisStep = new ArrayList<>(channelsToMergePerStep);
		int channelNum = numNotMerged;
		while (!closed && channelNum < channelIDs.size()) {
			channelsToMergeThisStep.clear();

			for (int i = 0; i < channelsToMergePerStep && channelNum < channelIDs.size(); i++, channelNum++) {
				channelsToMergeThisStep.add(channelIDs.get(channelNum));
			}

			mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep));
		}

		return mergedChannelIDs;
	}

	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run.
	 *
	 * @param channelIDs   The IDs of the runs' channels.
	 * @return The ID and number of blocks of the channel that describes the merged run.
	 */
	private ChannelWithMeta mergeChannels(List<ChannelWithMeta> channelIDs) throws IOException {
		// the list with the target iterators
		List<FileIOChannel> openChannels = new ArrayList<>(channelIDs.size());
		final BinaryMergeIterator<Entry> mergeIterator =
				getMergingIterator(channelIDs, openChannels);

		// create a new channel writer
		final FileIOChannel.ID mergedChannelID = ioManager.createChannel();
		channelManager.addChannel(mergedChannelID);
		AbstractChannelWriterOutputView output = null;

		int numBytesInLastBlock;
		int numBlocksWritten;
		try {
			output = FileChannelUtil.createOutputView(
					ioManager, mergedChannelID, compressionEnable,
					compressionCodecFactory, compressionBlockSize, pageSize);
			writeMergingOutput(mergeIterator, output);
			numBytesInLastBlock = output.close();
			numBlocksWritten = output.getBlockCount();
		} catch (IOException e) {
			if (output != null) {
				output.close();
				output.getChannel().deleteChannel();
			}
			throw e;
		}

		// remove, close and delete channels
		for (FileIOChannel channel : openChannels) {
			channelManager.removeChannel(channel.getChannelID());
			try {
				channel.closeAndDelete();
			} catch (Throwable ignored) {
			}
		}

		return new ChannelWithMeta(mergedChannelID, numBlocksWritten, numBytesInLastBlock);
	}

	// -------------------------------------------------------------------------------------------

	/**
	 * @return entry iterator reading from inView.
	 */
	protected abstract MutableObjectIterator<Entry> channelReaderInputViewIterator(
			AbstractChannelReaderInputView inView);

	/**
	 * @return merging comparator used in merging.
	 */
	protected abstract Comparator<Entry> mergeComparator();

	/**
	 * @return reused entry object used in merging.
	 */
	protected abstract List<Entry> mergeReusedEntries(int size);

	/**
	 * read the merged stream and write the data back.
	 */
	protected abstract void writeMergingOutput(
			MutableObjectIterator<Entry> mergeIterator,
			AbstractPagedOutputView output) throws IOException;
}
