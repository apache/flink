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
package eu.stratosphere.pact.common.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Base class for all input formats that use blocks of fixed size. The input splits are aligned to these blocks. Without
 * configuration, these block sizes equal the native block sizes of the HDFS.
 * 
 * @author Arvid Heise
 */
public abstract class BinaryInputFormat extends FileInputFormat {

	/**
	 * The log.
	 */
	private static final Log LOG = LogFactory.getLog(BinaryInputFormat.class);

	/**
	 * The config parameter which defines the fixed length of a record.
	 */
	public static final String BLOCK_SIZE_PARAMETER_KEY = "pact.input.block_size";

	public static final long NATIVE_BLOCK_SIZE = Long.MIN_VALUE;

	/**
	 * The block size to use.
	 */
	private long blockSize = NATIVE_BLOCK_SIZE;

	private DataInputStream dataInputStream;

	private BlockBasedInput blockBasedInput;

	private BlockInfo blockInfo;

	private long readRecords;

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.FileInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		// read own parameters
		this.blockSize = parameters.getLong(BLOCK_SIZE_PARAMETER_KEY, NATIVE_BLOCK_SIZE);
		if (this.blockSize < 1 && this.blockSize != NATIVE_BLOCK_SIZE)
			throw new IllegalArgumentException("The block size parameter must be set and larger than 0.");
		if (this.blockSize > Integer.MAX_VALUE)
			throw new UnsupportedOperationException("Currently only block size up to Integer.MAX_VALUE are supported");
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileInputFormat#createInputSplits(int)
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		List<FileStatus> files = this.getFiles();

		final FileSystem fs = this.filePath.getFileSystem();
		final long blockSize = this.blockSize == NATIVE_BLOCK_SIZE ? fs.getDefaultBlockSize() : this.blockSize;

		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);
		for (FileStatus file : files) {
			long splitSize = blockSize;
			for (long pos = 0, length = file.getLen(); pos < length; pos += splitSize) {
				long remainingLength = Math.min(pos + splitSize, length) - pos;

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, pos, remainingLength);
				Arrays.sort(blocks);

				inputSplits.add(new FileInputSplit(inputSplits.size(), file.getPath(), pos, remainingLength,
					blocks[0].getHosts()));
			}
		}

		if (files.size() < minNumSplits) {
			LOG.warn(String.format(
				"With the given block size %d, the file %s cannot be split into %d blocks. Filling up with empty splits...",
				blockSize, this.filePath, minNumSplits));
			FileStatus last = files.get(files.size() - 1);
			final BlockLocation[] blocks = fs.getFileBlockLocations(last, last.getLen(), 0);
			for (int index = files.size(); index < minNumSplits; index++)
				inputSplits.add(new FileInputSplit(index, last.getPath(), last.getLen(), 0, blocks[0].getHosts()));
		}

		return inputSplits.toArray(new FileInputSplit[0]);
	}

	protected List<FileStatus> getFiles() throws IOException {
		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();

		final FileSystem fs = this.filePath.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(this.filePath);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] partials = fs.listStatus(this.filePath);
			for (int i = 0; i < partials.length; i++)
				if (!partials[i].isDir())
					files.add(partials[i]);
		} else
			files.add(pathFile);

		return files;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.InputFormat#getStatistics(eu.stratosphere.pact.common.io.statistics.BaseStatistics
	 * )
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {

		// check the cache
		SequentialStatistics stats = null;

		if (cachedStatistics != null && cachedStatistics instanceof SequentialStatistics)
			stats = (SequentialStatistics) cachedStatistics;
		else
			stats = new SequentialStatistics(-1, BaseStatistics.UNKNOWN, BaseStatistics.UNKNOWN);

		try {
			boolean modified = false;
			List<FileStatus> files = this.getFiles();
			for (FileStatus fileStatus : files)
				if (fileStatus.getModificationTime() > stats.getLastModificationTime()) {
					stats.setLastModificationTime(fileStatus.getModificationTime());
					modified = true;
				}

			if (!modified)
				return stats;

			int totalLength = 0;
			// calculate the whole length
			for (FileStatus fileStatus : files)
				totalLength += fileStatus.getLen();

			stats.setTotalInputSize(totalLength);
			stats.setAverageRecordWidth(BaseStatistics.UNKNOWN);
			this.fillStatistics(files, stats);

		} catch (IOException ioex) {
			if (LOG.isWarnEnabled())
				LOG.warn(String.format("Could not determine complete statistics for file '%s' due to an I/O error: %s",
					this.filePath, StringUtils.stringifyException(ioex)));
		} catch (Throwable t) {
			if (LOG.isErrorEnabled())
				LOG.error(String.format("Unexpected problem while getting the file statistics for file '%s' due to %s",
					this.filePath, StringUtils.stringifyException(t)));
		}
		// sanity check
		if (stats.getTotalInputSize() <= 0)
			stats.setLastModificationTime(BaseStatistics.UNKNOWN);

		return stats;
	}

	protected FileInputSplit[] getInputSplits() throws IOException {
		return this.createInputSplits(0);
	}

	protected BlockInfo createBlockInfo() {
		return new BlockInfo();
	}

	/**
	 * Fill in the statistics. The last modification time and the total input size are prefilled.
	 * 
	 * @param files
	 *        The files that are associated with this block input format.
	 * @param stats
	 *        The pre-filled statistics.
	 */
	protected void fillStatistics(List<FileStatus> files, SequentialStatistics stats) throws IOException {
		if (files.isEmpty())
			return;

		BlockInfo blockInfo = this.createBlockInfo();

		long totalCount = 0;
		for (FileStatus file : files) {
			// invalid file
			if (file.getLen() < blockInfo.getInfoSize())
				continue;

			FSDataInputStream fdis = file.getPath().getFileSystem().open(file.getPath(), blockInfo.getInfoSize());
			fdis.seek(file.getLen() - blockInfo.getInfoSize());

			DataInputStream input = new DataInputStream(fdis);
			blockInfo.read(input);
			totalCount += blockInfo.getAccumulatedRecordCount();
		}

		stats.setNumberOfRecords(totalCount);
		stats.setAverageRecordWidth(totalCount == 0 ? 0 : ((float) stats.getTotalInputSize() / totalCount));
	}

	private static class SequentialStatistics extends FileBaseStatistics {
		private long numberOfRecords = UNKNOWN;

		public SequentialStatistics(long fileModTime, long fileSize, float avgBytesPerRecord) {
			super(fileModTime, fileSize, avgBytesPerRecord);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.FileInputFormat.FileBaseStatistics#getNumberOfRecords()
		 */
		@Override
		public long getNumberOfRecords() {
			return numberOfRecords;
		}

		/**
		 * Sets the numberOfRecords to the specified value.
		 * 
		 * @param numberOfRecords
		 *        the numberOfRecords to set
		 */
		public void setNumberOfRecords(long numberOfRecords) {
			this.numberOfRecords = numberOfRecords;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileInputFormat#open(eu.stratosphere.nephele.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException
	{
		super.open(split);

		final long blockSize = this.blockSize == NATIVE_BLOCK_SIZE ?
			this.filePath.getFileSystem().getDefaultBlockSize() : this.blockSize;

		this.blockInfo = this.createBlockInfo();
		if (this.splitLength > this.blockInfo.getInfoSize()) {
			this.stream.seek(this.splitStart + this.splitLength - this.blockInfo.getInfoSize());
			DataInputStream infoStream = new DataInputStream(this.stream);
			this.blockInfo.read(infoStream);
		}

		this.stream.seek(this.splitStart + this.blockInfo.getFirstRecordStart());
		this.blockBasedInput = new BlockBasedInput(this.stream, (int) blockSize);
		this.dataInputStream = new DataInputStream(this.blockBasedInput);
		this.readRecords = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#reachedEnd()
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return this.readRecords >= this.blockInfo.getRecordCount();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#nextRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException {
		if (this.reachedEnd())
			return false;
		this.deserialize(record, this.dataInputStream);
		this.readRecords++;
		return true;
	}

	protected abstract void deserialize(PactRecord record, DataInput dataInput) throws IOException;

	/**
	 * Writes a block info at the end of the blocks.<br>
	 * Current implementation uses only int and not long.
	 * 
	 * @author Arvid Heise
	 */
	protected class BlockBasedInput extends FilterInputStream {
		private final int maxPayloadSize;

		private int blockPos;

		public BlockBasedInput(FSDataInputStream in, int blockSize) {
			super(in);
			this.blockPos = (int) BinaryInputFormat.this.blockInfo.getFirstRecordStart();
			this.maxPayloadSize = blockSize - BinaryInputFormat.this.blockInfo.getInfoSize();
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.FilterInputStream#read()
		 */
		@Override
		public int read() throws IOException {
			if (this.blockPos++ >= this.maxPayloadSize)
				this.skipHeader();
			return in.read();
		}

		private void skipHeader() throws IOException {
			byte[] dummy = new byte[BinaryInputFormat.this.blockInfo.getInfoSize()];
			in.read(dummy, 0, dummy.length);
			this.blockPos = 0;
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.FilterInputStream#read(byte[])
		 */
		@Override
		public int read(byte[] b) throws IOException {
			return this.read(b, 0, b.length);
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.FilterInputStream#read(byte[], int, int)
		 */
		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int totalRead = 0;
			for (int remainingLength = len, offset = off; remainingLength > 0;) {
				int blockLen = Math.min(remainingLength, this.maxPayloadSize - this.blockPos);
				int read = in.read(b, offset, blockLen);
				if (read < 0)
					return read;
				totalRead += read;
				this.blockPos += read;
				offset += read;
				if (this.blockPos >= this.maxPayloadSize)
					this.skipHeader();
				remainingLength -= read;
			}
			return totalRead;
		}
	}
}
