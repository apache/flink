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

package org.apache.flink.api.common.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * The base class for {@link InputFormat}s that read from files. For specific input types the 
 * <tt>nextRecord()</tt> and <tt>reachedEnd()</tt> methods need to be implemented.
 * Additionally, one may override {@link #open(FileInputSplit)} and {@link #close()} to
 * change the life cycle behavior.
 * <p>
 * After the {@link #open(FileInputSplit)} method completed, the file input data is available
 * from the {@link #stream} field.
 */
public abstract class FileInputFormat<OT> implements InputFormat<OT, FileInputSplit> {
	
	// -------------------------------------- Constants -------------------------------------------
	
	private static final Logger LOG = LoggerFactory.getLogger(FileInputFormat.class);
	
	private static final long serialVersionUID = 1L;
	
	
	/**
	 * The fraction that the last split may be larger than the others.
	 */
	private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;
	
	/**
	 * The timeout (in milliseconds) to wait for a filesystem stream to respond.
	 */
	private static long DEFAULT_OPENING_TIMEOUT;

	/**
	 * Files with that suffix are unsplittable at a file level
	 * and compressed.
	 */
	protected static final String DEFLATE_SUFFIX = ".deflate";
	
	/**
	 * The splitLength is set to -1L for reading the whole split.
	 */
	protected static final long READ_WHOLE_SPLIT_FLAG = -1L;
	
	static {
		initDefaultsFromConfiguration();
	}
	
	private static void initDefaultsFromConfiguration() {
		
		final long to = GlobalConfiguration.getLong(ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
			ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
		if (to < 0) {
			LOG.error("Invalid timeout value for filesystem stream opening: " + to + ". Using default value of " +
				ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
			DEFAULT_OPENING_TIMEOUT = ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT;
		} else if (to == 0) {
			DEFAULT_OPENING_TIMEOUT = 300000; // 5 minutes
		} else {
			DEFAULT_OPENING_TIMEOUT = to;
		}
	}
	
	static long getDefaultOpeningTimeout() {
		return DEFAULT_OPENING_TIMEOUT;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Variables for internal operation.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The input stream reading from the input file.
	 */
	protected transient FSDataInputStream stream;

	/**
	 * The start of the split that this parallel instance must consume.
	 */
	protected transient long splitStart;

	/**
	 * The length of the split that this parallel instance must consume.
	 */
	protected transient long splitLength;
	
	
	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The path to the file that contains the input.
	 */
	protected Path filePath;
	
	/**
	 * The minimal split size, set by the configure() method.
	 */
	protected long minSplitSize = 0; 
	
	/**
	 * The desired number of splits, as set by the configure() method.
	 */
	protected int numSplits = -1;
	
	/**
	 * Stream opening timeout.
	 */
	protected long openTimeout = DEFAULT_OPENING_TIMEOUT;
	
	/**
	 * Some file input formats are not splittable on a block level (avro, deflate)
	 * Therefore, the FileInputFormat can only read whole files.
	 */
	protected boolean unsplittable = false;

	/**
	 * The flag to specify whether recursive traversal of the input directory
	 * structure is enabled.
	 */
	protected boolean enumerateNestedFiles = false;
	
	// --------------------------------------------------------------------------------------------
	//  Constructors
	// --------------------------------------------------------------------------------------------	

	public FileInputFormat() {}
	
	protected FileInputFormat(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("The file path must not be null.");
		}
		this.filePath = filePath;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------
	
	public Path getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		// TODO The job-submission web interface passes empty args (and thus empty
		// paths) to compute the preview graph. The following is a workaround for
		// this situation and we should fix this.
		if (filePath.isEmpty()) {
			setFilePath(new Path());
			return;
		}
		
		setFilePath(new Path(filePath));
	}
	
	public void setFilePath(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.filePath = filePath;
	}
	
	public long getMinSplitSize() {
		return minSplitSize;
	}
	
	public void setMinSplitSize(long minSplitSize) {
		if (minSplitSize < 0) {
			throw new IllegalArgumentException("The minimum split size cannot be negative.");
		}
		
		this.minSplitSize = minSplitSize;
	}
	
	public int getNumSplits() {
		return numSplits;
	}
	
	public void setNumSplits(int numSplits) {
		if (numSplits < -1 || numSplits == 0) {
			throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
		}
		
		this.numSplits = numSplits;
	}
	
	public long getOpenTimeout() {
		return openTimeout;
	}
	
	public void setOpenTimeout(long openTimeout) {
		if (openTimeout < 0) {
			throw new IllegalArgumentException("The timeout for opening the input splits must be positive or zero (= infinite).");
		}
		this.openTimeout = openTimeout;
	}

	// --------------------------------------------------------------------------------------------
	// Getting information about the split that is currently open
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the start of the current split.
	 *
	 * @return The start of the split.
	 */
	public long getSplitStart() {
		return splitStart;
	}
	
	/**
	 * Gets the length or remaining length of the current split.
	 *
	 * @return The length or remaining length of the current split.
	 */
	public long getSplitLength() {
		return splitLength;
	}

	// --------------------------------------------------------------------------------------------
	//  Pre-flight: Configuration, Splits, Sampling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures the file input format by reading the file path from the configuration.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#configure(org.apache.flink.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {
		// get the file path
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath != null) {
			try {
				this.filePath = new Path(filePath);
			}
			catch (RuntimeException rex) {
				throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
			}
		}
		else if (this.filePath == null) {
			throw new IllegalArgumentException("File path was not specified in input format, or configuration."); 
		}
		
		this.enumerateNestedFiles = parameters.getBoolean(ENUMERATE_NESTED_FILES_FLAG, false);
	}
	
	/**
	 * Obtains basic file statistics containing only file size. If the input is a directory, then the size is the sum of all contained files.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#getStatistics(org.apache.flink.api.common.io.statistics.BaseStatistics)
	 */
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		
		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
			(FileBaseStatistics) cachedStats : null;
				
		try {
			final Path path = this.filePath;
			final FileSystem fs = FileSystem.get(path.toUri());
			
			return getFileStats(cachedFileStats, path, fs, new ArrayList<FileStatus>(1));
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Could not determine statistics for file '" + this.filePath + "' due to an io error: "
						+ ioex.getMessage());
			}
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Unexpected problem while getting the file statistics for file '" + this.filePath + "': "
						+ t.getMessage(), t);
			}
		}
		
		// no statistics available
		return null;
	}
	
	protected FileBaseStatistics getFileStats(FileBaseStatistics cachedStats, Path filePath, FileSystem fs,
			ArrayList<FileStatus> files) throws IOException {
		
		// get the file info and check whether the cached statistics are still valid.
		final FileStatus file = fs.getFileStatus(filePath);
		long latestModTime = file.getModificationTime();
		long totalLength = 0;

		// enumerate all files and check their modification time stamp.
		if (file.isDir()) {
			FileStatus[] fss = fs.listStatus(filePath);
			files.ensureCapacity(fss.length);
			
			for (FileStatus s : fss) {
				if (!s.isDir()) {
					if (acceptFile(s)) {
						files.add(s);
						totalLength += s.getLen();
						latestModTime = Math.max(s.getModificationTime(), latestModTime);
						testForUnsplittable(s);
					}
				}
				else {
					if (enumerateNestedFiles && acceptFile(s)) {
						totalLength += addNestedFiles(s.getPath(), files, 0, false);
					}
				}
			}
		} else {
			files.add(file);
			testForUnsplittable(file);
			totalLength += file.getLen();
		}

		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}

		// sanity check
		if (totalLength <= 0) {
			totalLength = BaseStatistics.SIZE_UNKNOWN;
		}
		return new FileBaseStatistics(latestModTime, totalLength, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}

	@Override
	public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] splits) {
		return new LocatableInputSplitAssigner(splits);
	}

	/**
	 * Computes the input splits for the file. By default, one file block is one split. If more splits
	 * are requested than blocks are available, then a split may be a fraction of a block and splits may cross
	 * block boundaries.
	 * 
	 * @param minNumSplits The minimum desired number of file splits.
	 * @return The computed file splits.
	 * 
	 * @see org.apache.flink.api.common.io.InputFormat#createInputSplits(int)
	 */
	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}
		
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
				if (dir[i].isDir()) {
					if (enumerateNestedFiles) {
						if(acceptFile(dir[i])) {
							totalLength += addNestedFiles(dir[i].getPath(), files, 0, true);
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Directory "+dir[i].getPath().toString()+" did not pass the file-filter and is excluded.");
							}
						}
					}
				}
				else {
					if (acceptFile(dir[i])) {
						files.add(dir[i]);
						totalLength += dir[i].getLen();
						// as soon as there is one deflate file in a directory, we can not split it
						testForUnsplittable(dir[i]);
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("File "+dir[i].getPath().toString()+" did not pass the file-filter and is excluded.");
						}
					}
				}
			}
		} else {
			testForUnsplittable(pathFile);

			files.add(pathFile);
			totalLength += pathFile.getLen();
		}
		// returns if unsplittable
		if(unsplittable) {
			int splitNum = 0;
			for (final FileStatus file : files) {
				final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
				Set<String> hosts = new HashSet<String>();
				for(BlockLocation block : blocks) {
					hosts.addAll(Arrays.asList(block.getHosts()));
				}
				long len = file.getLen();
				if(testForUnsplittable(file)) {
					len = READ_WHOLE_SPLIT_FLAG;
				}
				FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, len,
						hosts.toArray(new String[hosts.size()]));
				inputSplits.add(fis);
			}
			return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
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
				if (LOG.isWarnEnabled()) {
					LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of " + 
						blockSize + ". Decreasing minimal split size to block size.");
				}
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
	 * Recursively traverse the input directory structure
	 * and enumerate all accepted nested files.
	 * @return the total length of accepted files.
	 */
	private long addNestedFiles(Path path, List<FileStatus> files, long length, boolean logExcludedFiles)
			throws IOException {
		final FileSystem fs = path.getFileSystem();

		for(FileStatus dir: fs.listStatus(path)) {
			if (dir.isDir()) {
				if (acceptFile(dir)) {
					addNestedFiles(dir.getPath(), files, length, logExcludedFiles);
				} else {
					if (logExcludedFiles && LOG.isDebugEnabled()) {
						LOG.debug("Directory "+dir.getPath().toString()+" did not pass the file-filter and is excluded.");
					}
				}
			}
			else {
				if(acceptFile(dir)) {
					files.add(dir);
					length += dir.getLen();
					testForUnsplittable(dir);
				} else {
					if (logExcludedFiles && LOG.isDebugEnabled()) {
						LOG.debug("Directory "+dir.getPath().toString()+" did not pass the file-filter and is excluded.");
					}
				}
			}
		}
		return length;
	}

	protected boolean testForUnsplittable(FileStatus pathFile) {
		if(pathFile.getPath().getName().endsWith(DEFLATE_SUFFIX)) {
			unsplittable = true;
			return true;
		}
		return false;
	}

	/**
	 * A simple hook to filter files and directories from the input.
	 * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
	 * same filters by default.
	 * 
	 * @param fileStatus
	 * @return true, if the given file or directory is accepted
	 */
	protected boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".");
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
	private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
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
	 */
	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		
		this.splitStart = fileSplit.getStart();
		this.splitLength = fileSplit.getLength();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");
		}

		
		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
		isot.start();
		
		try {
			this.stream = isot.waitForCompletion();
			this.stream = decorateInputStream(this.stream, fileSplit);
		}
		catch (Throwable t) {
			throw new IOException("Error opening the Input Split " + fileSplit.getPath() + 
					" [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
		}
		
		// get FSDataInputStream
		if (this.splitStart != 0) {
			this.stream.seek(this.splitStart);
		}
	}

	/**
	 * This method allows to wrap/decorate the raw {@link FSDataInputStream} for a certain file split, e.g., for decoding.
	 * When overriding this method, also consider adapting {@link FileInputFormat#testForUnsplittable} if your
	 * stream decoration renders the input file unsplittable. Also consider calling existing superclass implementations.
	 *
	 * @param inputStream is the input stream to decorated
	 * @param fileSplit   is the file split for which the input stream shall be decorated
	 * @return the decorated input stream
	 * @throws Throwable if the decoration fails
	 * @see org.apache.flink.api.common.io.InputStreamFSInputWrapper
	 */
	protected FSDataInputStream decorateInputStream(FSDataInputStream inputStream, FileInputSplit fileSplit) throws Throwable {
		// Wrap stream in a extracting (decompressing) stream if file ends with .deflate.
		if (fileSplit.getPath().getName().endsWith(DEFLATE_SUFFIX)) {
			return new InflaterInputStreamFSInputWrapper(stream);
		}

		return inputStream;
	}

	/**
	 * Closes the file input stream of the input format.
	 */
	@Override
	public void close() throws IOException {
		if (this.stream != null) {
			// close input stream
			this.stream.close();
			stream = null;
		}
	}
	

	public String toString() {
		return this.filePath == null ? 
			"File Input (unknown file)" :
			"File Input (" + this.filePath.toString() + ')';
	}
	
	// ============================================================================================
	
	/**
	 * Encapsulation of the basic statistics the optimizer obtains about a file. Contained are the size of the file
	 * and the average bytes of a single record. The statistics also have a time-stamp that records the modification
	 * time of the file and indicates as such for which time the statistics were valid.
	 */
	public static class FileBaseStatistics implements BaseStatistics {
		
		protected final long fileModTime; // timestamp of the last modification

		protected final long fileSize; // size of the file(s) in bytes

		protected final float avgBytesPerRecord; // the average number of bytes for a record

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
		 * Gets the file size.
		 * 
		 * @return The fileSize.
		 * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getTotalInputSize()
		 */
		@Override
		public long getTotalInputSize() {
			return this.fileSize;
		}

		/**
		 * Gets the estimates number of records in the file, computed as the file size divided by the
		 * average record width, rounded up.
		 * 
		 * @return The estimated number of records in the file.
		 * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getNumberOfRecords()
		 */
		@Override
		public long getNumberOfRecords() {
			return (this.fileSize == SIZE_UNKNOWN || this.avgBytesPerRecord == AVG_RECORD_BYTES_UNKNOWN) ? 
				NUM_RECORDS_UNKNOWN : (long) Math.ceil(this.fileSize / this.avgBytesPerRecord);
		}

		/**
		 * Gets the estimated average number of bytes per record.
		 * 
		 * @return The average number of bytes per record.
		 * @see org.apache.flink.api.common.io.statistics.BaseStatistics#getAverageRecordWidth()
		 */
		@Override
		public float getAverageRecordWidth() {
			return this.avgBytesPerRecord;
		}
		
		@Override
		public String toString() {
			return "size=" + this.fileSize + ", recWidth=" + this.avgBytesPerRecord + ", modAt=" + this.fileModTime;
		}
	}
	
	// ============================================================================================
	
	/**
	 * Obtains a DataInputStream in an thread that is not interrupted.
	 * This is a necessary hack around the problem that the HDFS client is very sensitive to InterruptedExceptions.
	 */
	public static class InputSplitOpenThread extends Thread {
		
		private final FileInputSplit split;
		
		private final long timeout;

		private volatile FSDataInputStream fdis;

		private volatile Throwable error;
		
		private volatile boolean aborted;

		public InputSplitOpenThread(FileInputSplit split, long timeout) {
			super("Transient InputSplit Opener");
			setDaemon(true);
			
			this.split = split;
			this.timeout = timeout;
		}

		@Override
		public void run() {
			try {
				final FileSystem fs = FileSystem.get(this.split.getPath().toUri());
				this.fdis = fs.open(this.split.getPath());
				
				// check for canceling and close the stream in that case, because no one will obtain it
				if (this.aborted) {
					final FSDataInputStream f = this.fdis;
					this.fdis = null;
					f.close();
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
		}
		
		public FSDataInputStream waitForCompletion() throws Throwable {
			final long start = System.currentTimeMillis();
			long remaining = this.timeout;
			
			do {
				try {
					// wait for the task completion
					this.join(remaining);
				}
				catch (InterruptedException iex) {
					// we were canceled, so abort the procedure
					abortWait();
					throw iex;
				}
			}
			while (this.error == null && this.fdis == null &&
					(remaining = this.timeout + start - System.currentTimeMillis()) > 0);
			
			if (this.error != null) {
				throw this.error;
			}
			if (this.fdis != null) {
				return this.fdis;
			} else {
				// double-check that the stream has not been set by now. we don't know here whether
				// a) the opener thread recognized the canceling and closed the stream
				// b) the flag was set such that the stream did not see it and we have a valid stream
				// In any case, close the stream and throw an exception.
				abortWait();
				
				final boolean stillAlive = this.isAlive();
				final StringBuilder bld = new StringBuilder(256);
				for (StackTraceElement e : this.getStackTrace()) {
					bld.append("\tat ").append(e.toString()).append('\n');
				}
				throw new IOException("Input opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") + 
					" alive. Stack of split open thread:\n" + bld.toString());
			}
		}
		
		/**
		 * Double checked procedure setting the abort flag and closing the stream.
		 */
		private void abortWait() {
			this.aborted = true;
			final FSDataInputStream inStream = this.fdis;
			this.fdis = null;
			if (inStream != null) {
				try {
					inStream.close();
				} catch (Throwable t) {}
			}
		}
	}
	
	// ============================================================================================
	//  Parameterization via configuration
	// ============================================================================================
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	/**
	 * The config parameter which defines the input file path.
	 */
	private static final String FILE_PARAMETER_KEY = "input.file.path";

	/**
	 * The config parameter which defines whether input directories are recursively traversed.
	 */
	private static final String ENUMERATE_NESTED_FILES_FLAG = "recursive.file.enumeration";
	
	
	// ----------------------------------- Config Builder -----------------------------------------
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureFileFormat(GenericDataSourceBase<?, ?> target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * Abstract builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static abstract class AbstractConfigBuilder<T> {
		/**
		 * The configuration into which the parameters will be written.
		 */
		protected final Configuration config;
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration targetConfig) {
			this.config = targetConfig;
		}
		
		// --------------------------------------------------------------------
		
		/**
		 * Sets the path to the file or directory to be read by this file input format.
		 * 
		 * @param filePath The path to the file or directory.
		 * @return The builder itself.
		 */
		public T filePath(String filePath) {
			this.config.setString(FILE_PARAMETER_KEY, filePath);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
	}
	
	/**
	 * A builder used to set parameters to the input format's configuration in a fluent way.
	 */
	public static class ConfigBuilder extends AbstractConfigBuilder<ConfigBuilder> {
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected ConfigBuilder(Configuration targetConfig) {
			super(targetConfig);
		}
		
	}
}
