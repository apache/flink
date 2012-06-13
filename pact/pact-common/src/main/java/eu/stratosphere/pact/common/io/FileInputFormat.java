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
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;


/**
 * Describes the base interface that is used for reading from a file input. For specific input types the 
 * <tt>nextRecord()</tt> and <tt>reachedEnd()</tt> methods need to be implemented. Additionally, one may override
 * <tt>open(FileInputSplit)</tt> and <tt>close()</tt> to
 * 
 * 
 * 
 * While reading the runtime checks whether the end was reached using reachedEnd()
 * and if not the next pair is read using the nextPair() method.
 * 
 * Describes the base interface that is used describe an input that produces records that are processed
 * by stratosphere.
 * <p>
 * The input format handles the following:
 * <ul>
 *   <li>It describes how the input is split into splits that can be processed in parallel.</li>
 *   <li>It describes how to read records from the input split.</li>
 *   <li>It describes how to gather basic statistics from the input.</li> 
 * </ul>
 * <p>
 * The life cycle of an input format is the following:
 * <ol>
 *   <li>After being instantiated (parameterless), it is configured with a {@link Configuration} object. 
 *       Basic fields are read from the configuration, such as for example a file path, if the format describes
 *       files as input.</li>
 *   <li>It is called to create the input splits.</li>
 *   <li>Optionally: It is called by the compiler to produce basic statistics about the input.</li>
 *   <li>Each parallel input task creates an instance, configures it and opens it for a specific split.</li>
 *   <li>All records are read from the input</li>
 *   <li>The input format is closed</li>
 * </ol>
 * 
 * 
 * @author Moritz Kaufmann
 * @author Stephan Ewen
 */
public abstract class FileInputFormat implements InputFormat<PactRecord, FileInputSplit>
{
	/**
	 * The config parameter which defines the input file path.
	 */
	public static final String FILE_PARAMETER_KEY = "pact.input.file.path";
	
	/**
	 * The config parameter which defines the number of desired splits.
	 */
	public static final String DESIRED_NUMBER_OF_SPLITS_PARAMETER_KEY = "pact.input.file.numsplits";
	
	/**
	 * The config parameter for the minimal split size.
	 */
	public static final String MINIMAL_SPLIT_SIZE_PARAMETER_KEY = "pact.input.file.minsplitsize";
	
	
	/**
	 * The LOG for logging messages in this class.
	 */
	private static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	
	/**
	 * The maximal time that the format waits for a split to be opened before declaring failure.
	 */
	private static final long OPEN_TIMEOUT_MILLIES = 10000;
	
	/**
	 * The fraction that the last split may be larger than the others.
	 */
	private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The path to the file that contains the input.
	 */
	protected Path filePath;
	
	/**
	 * The input stream reading from the input file.
	 */
	protected FSDataInputStream stream;

	/**
	 * The start of the split that this parallel instance must consume.
	 */
	protected long splitStart;

	/**
	 * The length of the split that this parallel instance must consume.
	 */
	protected long splitLength;
	
	/**
	 * The the minimal split size, set by the configure() method.
	 */
	protected long minSplitSize; 
	
	/**
	 * The desired number of splits, as set by the configure() method.
	 */
	protected int numSplits;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures the file input format by reading the file path from the configuration.
	 * 
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		// get the file path
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath == null) {
			throw new IllegalArgumentException("Configuration file FileInputFormat does not contain the file path.");
		}
		
		try {
			this.filePath = new Path(filePath);
		}
		catch (RuntimeException rex) {
			throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
		}
		
		// get the number of splits
		this.numSplits = parameters.getInteger(DESIRED_NUMBER_OF_SPLITS_PARAMETER_KEY, -1);
		if (this.numSplits == 0 || this.numSplits < -1) {
			this.numSplits = -1;
			if (LOG.isWarnEnabled())
				LOG.warn("Ignoring invalid parameter for number of splits: " + this.numSplits);
		}
		
		// get the minimal split size
		this.minSplitSize = parameters.getLong(MINIMAL_SPLIT_SIZE_PARAMETER_KEY, 1);
		if (this.minSplitSize < 1) {
			this.minSplitSize = 1;
			if (LOG.isWarnEnabled())
				LOG.warn("Ignoring invalid parameter for minimal split size (requires a positive value): " + this.numSplits);
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getInputSplitType()
	 */
	@Override
	public Class<FileInputSplit> getInputSplitType()
	{
		return FileInputSplit.class;
	}

	/**
	 * Computes the input splits for the file. By default, one file block is one split. If more splits
	 * are requested than blocks are available, then a split may by a fraction of a block and splits may cross
	 * block boundaries.
	 * 
	 * @param The minimum desired number of file splits.
	 * @return The computed file splits.
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#createInputSplits(int)
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException
	{
		// take the desired number of splits into account
		minNumSplits = Math.max(minNumSplits, this.numSplits);
		
		final Path path = this.filePath;
		final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();
		long totalLength = 0;

		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] dir = fs.listStatus(path);
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir()) {
					files.add(dir[i]);
					totalLength += dir[i].getLen();
				}
			}
		} else {
			files.add(pathFile);
			totalLength += pathFile.getLen();
		}

		final long maxSplitSize = (minNumSplits < 1) ? Long.MAX_VALUE : (totalLength / minNumSplits +
					(totalLength % minNumSplits == 0 ? 0 : 1));

		// now that we have the files, generate the splits
		int splitNum = 0;
		for (final FileStatus file : files) {

			final long len = file.getLen();
			final long blockSize = file.getBlockSize();
			
			final long minSplitSize;
			if (this.minSplitSize <= blockSize) {
				minSplitSize = this.minSplitSize;
			}
			else {
				if (LOG.isWarnEnabled())
					LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " + 
						blockSize + ". Decreasing minimal split size to block size.");
				minSplitSize = blockSize;
			}

			final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
			final long halfSplit = splitSize >>> 1;

			final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

			if (len > 0) {

				// get the block locations and make sure they are in order with respect to their offset
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
				Arrays.sort(blocks);

				long bytesUnassigned = len;
				long position = 0;

				int blockIndex = 0;

				while (bytesUnassigned > maxBytesForLastSplit) {
					// get the block containing the majority of the data
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					// create a new split
					FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
						blocks[blockIndex].getHosts());
					inputSplits.add(fis);

					// adjust the positions
					position += splitSize;
					bytesUnassigned -= splitSize;
				}

				// assign the last split
				if (bytesUnassigned > 0) {
					blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
					final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
						bytesUnassigned, blocks[blockIndex].getHosts());
					inputSplits.add(fis);
				}
			} else {
				// special case with a file of zero bytes size
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
				String[] hosts;
				if (blocks.length > 0) {
					hosts = blocks[0].getHosts();
				} else {
					hosts = new String[0];
				}
				final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
				inputSplits.add(fis);
			}
		}

		return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
	}

	/**
	 * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file described by the given
	 * offset.
	 * 
	 * @param blocks The different blocks of the file. Must be ordered by their offset.
	 * @param offset The offset of the position in the file.
	 * @param startIndex The earliest index to look at.
	 * @return The index of the block containing the given position.
	 */
	private final int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex)
	{
		// go over all indexes after the startIndex
		for (int i = startIndex; i < blocks.length; i++) {
			long blockStart = blocks[i].getOffset();
			long blockEnd = blockStart + blocks[i].getLength();

			if (offset >= blockStart && offset < blockEnd) {
				// got the block where the split starts
				// check if the next block contains more than this one does
				if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
					return i + 1;
				} else {
					return i;
				}
			}
		}
		throw new IllegalArgumentException("The given offset is not contained in the any block.");
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Opens an input stream to the file defined in the input format.
	 * The stream is positioned at the beginning of the given split.
	 * <p>
	 * The stream is actually opened in an asynchronous thread to make sure any interruptions to the thread 
	 * working on the input format do not reach the file system.
	 * 
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#open(eu.stratosphere.nephele.template.InputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException
	{
		if (!(split instanceof FileInputSplit)) {
			throw new IllegalArgumentException("File Input Formats can only be used with FileInputSplits.");
		}
		
		final FileInputSplit fileSplit = (FileInputSplit) split;
		
		this.splitStart = fileSplit.getStart();
		this.splitLength = fileSplit.getLength();

		if (LOG.isDebugEnabled())
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");

		
		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, OPEN_TIMEOUT_MILLIES);
		isot.start();
		
		try {
			isot.waitForCompletion();
		}
		catch (Throwable t) {
			throw new IOException("Error opening the Input Split " + fileSplit.getPath() + 
					" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}

		// get FSDataInputStream
		this.stream = isot.getFSDataInputStream();
		this.stream.seek(this.splitStart);
	}
	
	/**
	 * Closes the file input stream of the input format.
	 */
	@Override
	public void close() throws IOException
	{
		if (this.stream != null) {
			// close input stream
			this.stream.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return this.filePath == null ? 
			"File Input (unknown file)" :
			"File Input (" + this.filePath.toString() + ')';
	}
	
// ============================================================================================
	
	/**
	 * Encapsulation of the basic statistics the optimizer obtains about a file. Contained are the size of the file
	 * and the average bytes of a single record. The statistics also have a time-stamp that records the modification
	 * time of the file and indicates as such for which time the statistics were valid.
	 *
	 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
	 */
	public static class FileBaseStatistics implements BaseStatistics
	{
		protected long fileModTime; // timestamp of the last modification

		protected long fileSize; // size of the file(s) in bytes

		protected float avgBytesPerRecord; // the average number of bytes for a record

		/**
		 * Creates a new statistics object.
		 * 
		 * @param fileModTime
		 *        The timestamp of the latest modification of any of the involved files.
		 * @param fileSize
		 *        The size of the file, in bytes. <code>-1</code>, if unknown.
		 * @param avgBytesPerRecord
		 *        The average number of byte in a record, or <code>-1.0f</code>, if unknown.
		 */
		public FileBaseStatistics(long fileModTime, long fileSize, float avgBytesPerRecord) {
			this.fileModTime = fileModTime;
			this.fileSize = fileSize;
			this.avgBytesPerRecord = avgBytesPerRecord;
		}

		/**
		 * Gets the timestamp of the last modification.
		 * 
		 * @return The timestamp of the last modification.
		 */
		public long getLastModificationTime() {
			return fileModTime;
		}
		
		/**
		 * Sets the timestamp of the last modification.
		 *
		 * @param modificationTime The timestamp of the last modification
		 */
		public void setLastModificationTime(long modificationTime) 
		{
			this.fileModTime = modificationTime;
		}

		/**
		 * Gets the file size.
		 * 
		 * @return The fileSize.
		 * @see eu.stratosphere.pact.common.io.statistics.BaseStatistics#getTotalInputSize()
		 */
		@Override
		public long getTotalInputSize()
		{
			return this.fileSize;
		}
		
		/**
		 * Sets the file size to the specified value.
		 *
		 * @param fileSize the fileSize to set
		 */
		public void setTotalInputSize(long fileSize) 
		{
			this.fileSize = fileSize;
		}

		/**
		 * Gets the estimates number of records in the file, computed as the file size divided by the
		 * average record width, rounded up.
		 * 
		 * @return The estimated number of records in the file.
		 * @see eu.stratosphere.pact.common.io.statistics.BaseStatistics#getNumberOfRecords()
		 */
		@Override
		public long getNumberOfRecords()
		{
			return (long) Math.ceil(this.fileSize / this.avgBytesPerRecord);
		}
		
		/**
		 * Sets the estimated average number of bytes per record.
		 *
		 * @param avgBytesPerRecord the average number of bytes per record
		 */
		public void setAverageRecordWidth(float avgBytesPerRecord) 
		{
			this.avgBytesPerRecord = avgBytesPerRecord;
		}

		/**
		 * Gets the estimated average number of bytes per record.
		 * 
		 * @return The average number of bytes per record.
		 * @see eu.stratosphere.pact.common.io.statistics.BaseStatistics#getAverageRecordWidth()
		 */
		@Override
		public float getAverageRecordWidth()
		{
			return this.avgBytesPerRecord;
		}
	}
	
	// ============================================================================================
	
	/**
	 * Obtains a DataInputStream in an thread that is not interrupted.
	 * This is a necessary hack around the problem that the HDFS client is very sensitive to InterruptedExceptions.
	 */
	public static class InputSplitOpenThread extends Thread
	{	
		private final FileInputSplit split;
		
		private final long timeout;

		private volatile FSDataInputStream fdis = null;

		private volatile Throwable error = null;

		public InputSplitOpenThread(FileInputSplit split, long timeout)
		{
			this.split = split;
			this.timeout = timeout;
		}

		@Override
		public void run() {
			try {
				FileSystem fs = FileSystem.get(this.split.getPath().toUri());
				this.fdis = fs.open(this.split.getPath());
			}
			catch (Throwable t) {
				this.error = t;
			}
		}
		
		public void waitForCompletion() throws Throwable
		{
			long start = System.currentTimeMillis();
			long remaining = this.timeout;
			
			if (this.error != null) {
				throw this.error;
			}
			if (this.fdis != null) {
				return;
			}
			
			do {
				try {
					this.join(remaining);
				} catch (InterruptedException iex) {}
			}
			while (this.error == null && this.fdis == null &&
					(remaining = this.timeout + start - System.currentTimeMillis()) > 0);
			
			if (this.error != null) {
				throw this.error;
			}
			if (this.fdis != null) {
				return;
			}
			
			// try to forcefully shut this thread down
			throw new IOException("Opening request timed out.");
		}

		public FSDataInputStream getFSDataInputStream() {
			return this.fdis;
		}
	}
}
