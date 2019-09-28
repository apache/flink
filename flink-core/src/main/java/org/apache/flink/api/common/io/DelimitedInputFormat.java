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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.OptimizerOptions;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Base implementation for input formats that split the input at a delimiter into records.
 * The parsing of the record bytes into the record has to be implemented in the
 * {@link #readRecord(Object, byte[], int, int)} method.
 * 
 * <p>The default delimiter is the newline character {@code '\n'}.</p>
 */
@Public
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT> implements CheckpointableInputFormat<FileInputSplit, Long> {
	
	private static final long serialVersionUID = 1L;

	// -------------------------------------- Constants -------------------------------------------
	
	/**
	 * The log.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(DelimitedInputFormat.class);

	// The charset used to convert strings to bytes
	private String charsetName = "UTF-8";

	// Charset is not serializable
	private transient Charset charset;

	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * Indication that the number of samples has not been set by the configuration.
	 */
	private static final int NUM_SAMPLES_UNDEFINED = -1;
	
	/**
	 * The maximum number of line samples to be taken.
	 */
	private static int DEFAULT_MAX_NUM_SAMPLES;
	
	/**
	 * The minimum number of line samples to be taken.
	 */
	private static int DEFAULT_MIN_NUM_SAMPLES;
	
	/**
	 * The maximum size of a sample record before sampling is aborted. To catch cases where a wrong delimiter is given.
	 */
	private static int MAX_SAMPLE_LEN;

	/**
	 * @deprecated Please use {@code loadConfigParameters(Configuration config}
	 */
	@Deprecated
	protected static void loadGlobalConfigParams() {
		loadConfigParameters(GlobalConfiguration.loadConfiguration());
	}

	protected static void loadConfigParameters(Configuration parameters) {
		int maxSamples = parameters.getInteger(OptimizerOptions.DELIMITED_FORMAT_MAX_LINE_SAMPLES);
		int minSamples = parameters.getInteger(OptimizerOptions.DELIMITED_FORMAT_MIN_LINE_SAMPLES);
		
		if (maxSamples < 0) {
			LOG.error("Invalid default maximum number of line samples: " + maxSamples + ". Using default value of " +
				OptimizerOptions.DELIMITED_FORMAT_MAX_LINE_SAMPLES.key());
			maxSamples = OptimizerOptions.DELIMITED_FORMAT_MAX_LINE_SAMPLES.defaultValue();
		}
		if (minSamples < 0) {
			LOG.error("Invalid default minimum number of line samples: " + minSamples + ". Using default value of " +
				OptimizerOptions.DELIMITED_FORMAT_MIN_LINE_SAMPLES.key());
			minSamples = OptimizerOptions.DELIMITED_FORMAT_MIN_LINE_SAMPLES.defaultValue();
		}
		
		DEFAULT_MAX_NUM_SAMPLES = maxSamples;
		
		if (minSamples > maxSamples) {
			LOG.error("Default minimum number of line samples cannot be greater the default maximum number " +
					"of line samples: min=" + minSamples + ", max=" + maxSamples + ". Defaulting minimum to maximum.");
			DEFAULT_MIN_NUM_SAMPLES = maxSamples;
		} else {
			DEFAULT_MIN_NUM_SAMPLES = minSamples;
		}
		
		int maxLen = parameters.getInteger(OptimizerOptions.DELIMITED_FORMAT_MAX_SAMPLE_LEN);
		if (maxLen <= 0) {
			maxLen = OptimizerOptions.DELIMITED_FORMAT_MAX_SAMPLE_LEN.defaultValue();
			LOG.error("Invalid value for the maximum sample record length. Using default value of " + maxLen + '.');
		} else if (maxLen < DEFAULT_READ_BUFFER_SIZE) {
			maxLen = DEFAULT_READ_BUFFER_SIZE;
			LOG.warn("Increasing maximum sample record length to size of the read buffer (" + maxLen + ").");
		}
		MAX_SAMPLE_LEN = maxLen;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Variables for internal parsing.
	//  They are all transient, because we do not want them so be serialized 
	// --------------------------------------------------------------------------------------------
	
	private transient byte[] readBuffer;

	private transient byte[] wrapBuffer;

	private transient int readPos;

	private transient int limit;

	private transient byte[] currBuffer;		// buffer in which current record byte sequence is found
	private transient int currOffset;			// offset in above buffer
	private transient int currLen;				// length of current byte sequence

	private transient boolean overLimit;

	private transient boolean end;

	private long offset = -1;

	// --------------------------------------------------------------------------------------------
	//  The configuration parameters. Configured on the instance and serialized to be shipped.
	// --------------------------------------------------------------------------------------------

	// The delimiter may be set with a byte-sequence or a String. In the latter
	// case the byte representation is updated consistent with current charset.
	private byte[] delimiter = new byte[] {'\n'};
	private String delimiterString = null;

	private int lineLengthLimit = Integer.MAX_VALUE;
	
	private int bufferSize = -1;
	
	private int numLineSamples = NUM_SAMPLES_UNDEFINED;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructors & Getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------

	public DelimitedInputFormat() {
		this(null, null);
	}

	protected DelimitedInputFormat(Path filePath, Configuration configuration) {
		super(filePath);
		if (configuration == null) {
			configuration = GlobalConfiguration.loadConfiguration();
		}
		loadConfigParameters(configuration);
	}

	/**
	 * Get the character set used for the row delimiter. This is also used by
	 * subclasses to interpret field delimiters, comment strings, and for
	 * configuring {@link FieldParser}s.
	 *
	 * @return the charset
	 */
	@PublicEvolving
	public Charset getCharset() {
		if (this.charset == null) {
			this.charset = Charset.forName(charsetName);
		}
		return this.charset;
	}

	/**
	 * Set the name of the character set used for the row delimiter. This is
	 * also used by subclasses to interpret field delimiters, comment strings,
	 * and for configuring {@link FieldParser}s.
	 *
	 * These fields are interpreted when set. Changing the charset thereafter
	 * may cause unexpected results.
	 *
	 * @param charset name of the charset
	 */
	@PublicEvolving
	public void setCharset(String charset) {
		this.charsetName = Preconditions.checkNotNull(charset);
		this.charset = null;

		if (this.delimiterString != null) {
			this.delimiter = delimiterString.getBytes(getCharset());
		}
	}

	public byte[] getDelimiter() {
		return delimiter;
	}
	
	public void setDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		this.delimiter = delimiter;
		this.delimiterString = null;
	}

	public void setDelimiter(char delimiter) {
		setDelimiter(String.valueOf(delimiter));
	}
	
	public void setDelimiter(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		this.delimiter = delimiter.getBytes(getCharset());
		this.delimiterString = delimiter;
	}
	
	public int getLineLengthLimit() {
		return lineLengthLimit;
	}
	
	public void setLineLengthLimit(int lineLengthLimit) {
		if (lineLengthLimit < 1) {
			throw new IllegalArgumentException("Line length limit must be at least 1.");
		}

		this.lineLengthLimit = lineLengthLimit;
	}
	
	public int getBufferSize() {
		return bufferSize;
	}
	
	public void setBufferSize(int bufferSize) {
		if (bufferSize < 2) {
			throw new IllegalArgumentException("Buffer size must be at least 2.");
		}

		this.bufferSize = bufferSize;
	}
	
	public int getNumLineSamples() {
		return numLineSamples;
	}
	
	public void setNumLineSamples(int numLineSamples) {
		if (numLineSamples < 0) {
			throw new IllegalArgumentException("Number of line samples must not be negative.");
		}
		this.numLineSamples = numLineSamples;
	}

	// --------------------------------------------------------------------------------------------
	//  User-defined behavior
	// --------------------------------------------------------------------------------------------

	/**
	 * This function parses the given byte array which represents a serialized record.
	 * The function returns a valid record or throws an IOException.
	 * 
	 * @param reuse An optionally reusable object.
	 * @param bytes Binary data of serialized records.
	 * @param offset The offset where to start to read the record data. 
	 * @param numBytes The number of bytes that can be read starting at the offset position.
	 * 
	 * @return Returns the read record if it was successfully deserialized.
	 * @throws IOException if the record could not be read.
	 */
	public abstract OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes) throws IOException;
	
	// --------------------------------------------------------------------------------------------
	//  Pre-flight: Configuration, Splits, Sampling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Configures this input format by reading the path to the file from the configuration and the string that
	 * defines the record delimiter.
	 * 
	 * @param parameters The configuration object to read the parameters from.
	 */
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		// the if() clauses are to prevent the configure() method from
		// overwriting the values set by the setters

		if (Arrays.equals(delimiter, new byte[] {'\n'})) {
			String delimString = parameters.getString(RECORD_DELIMITER, null);
			if (delimString != null) {
				setDelimiter(delimString);
			}
		}
		
		// set the number of samples
		if (numLineSamples == NUM_SAMPLES_UNDEFINED) {
			String samplesString = parameters.getString(NUM_STATISTICS_SAMPLES, null);
			if (samplesString != null) {
				try {
					setNumLineSamples(Integer.parseInt(samplesString));
				} catch (NumberFormatException e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Invalid value for number of samples to take: " + samplesString + ". Skipping sampling.");
					}
					setNumLineSamples(0);
				}
			}
		}
	}
	
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		
		final FileBaseStatistics cachedFileStats = cachedStats instanceof FileBaseStatistics ?
				(FileBaseStatistics) cachedStats : null;
		
		// store properties
		final long oldTimeout = this.openTimeout;
		final int oldBufferSize = this.bufferSize;
		final int oldLineLengthLimit = this.lineLengthLimit;
		try {

			final ArrayList<FileStatus> allFiles = new ArrayList<>(1);

			// let the file input format deal with the up-to-date check and the basic size
			final FileBaseStatistics stats = getFileStats(cachedFileStats, getFilePaths(), allFiles);
			if (stats == null) {
				return null;
			}
			
			// check whether the width per record is already known or the total size is unknown as well
			// in both cases, we return the stats as they are
			if (stats.getAverageRecordWidth() != FileBaseStatistics.AVG_RECORD_BYTES_UNKNOWN ||
					stats.getTotalInputSize() == FileBaseStatistics.SIZE_UNKNOWN) {
				return stats;
			}

			// disabling sampling for unsplittable files since the logic below assumes splitability.
			// TODO: Add sampling for unsplittable files. Right now, only compressed text files are affected by this limitation.
			if (unsplittable) {
				return stats;
			}
			
			// compute how many samples to take, depending on the defined upper and lower bound
			final int numSamples;
			if (this.numLineSamples != NUM_SAMPLES_UNDEFINED) {
				numSamples = this.numLineSamples;
			} else {
				// make the samples small for very small files
				final int calcSamples = (int) (stats.getTotalInputSize() / 1024);
				numSamples = Math.min(DEFAULT_MAX_NUM_SAMPLES, Math.max(DEFAULT_MIN_NUM_SAMPLES, calcSamples));
			}
			
			// check if sampling is disabled.
			if (numSamples == 0) {
				return stats;
			}
			if (numSamples < 0) {
				throw new RuntimeException("Error: Invalid number of samples: " + numSamples);
			}
			
			
			// make sure that the sampling times out after a while if the file system does not answer in time
			this.openTimeout = 10000;
			// set a small read buffer size
			this.bufferSize = 4 * 1024;
			// prevent overly large records, for example if we have an incorrectly configured delimiter
			this.lineLengthLimit = MAX_SAMPLE_LEN;
			
			long offset = 0;
			long totalNumBytes = 0;
			long stepSize = stats.getTotalInputSize() / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			while (samplesTaken < numSamples && fileNum < allFiles.size()) {
				// make a split for the sample and use it to read a record
				FileStatus file = allFiles.get(fileNum);
				FileInputSplit split = new FileInputSplit(0, file.getPath(), offset, file.getLen() - offset, null);

				// we open the split, read one line, and take its length
				try {
					open(split);
					if (readLine()) {
						totalNumBytes += this.currLen + this.delimiter.length;
						samplesTaken++;
					}
				} finally {
					// close the file stream, do not release the buffers
					super.close();
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < allFiles.size() && offset >= (file = allFiles.get(fileNum)).getLen()) {
					offset -= file.getLen();
					fileNum++;
				}
			}
			
			// we have the width, store it
			return new FileBaseStatistics(stats.getLastModificationTime(),
				stats.getTotalInputSize(), totalNumBytes / (float) samplesTaken);
			
		} catch (IOException ioex) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Could not determine statistics for files '" + Arrays.toString(getFilePaths()) + "' " +
						 "due to an io error: " + ioex.getMessage());
			}
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Unexpected problem while getting the file statistics for files '" + Arrays.toString(getFilePaths()) + "': "
						+ t.getMessage(), t);
			}
		} finally {
			// restore properties (even on return)
			this.openTimeout = oldTimeout;
			this.bufferSize = oldBufferSize;
			this.lineLengthLimit = oldLineLengthLimit;
		}
		
		// no statistics possible
		return null;
	}

	/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	 *
	 * @param split The input split to open.
	 *
	 * @see org.apache.flink.api.common.io.FileInputFormat#open(org.apache.flink.core.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		initBuffers();

		this.offset = splitStart;
		if (this.splitStart != 0) {
			this.stream.seek(offset);
			readLine();
			// if the first partial record already pushes the stream over
			// the limit of our split, then no record starts within this split
			if (this.overLimit) {
				this.end = true;
			}
		} else {
			fillBuffer(0);
		}
	}

	private void initBuffers() {
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

		if (this.bufferSize <= this.delimiter.length) {
			throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
		}

		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}
		if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
			this.wrapBuffer = new byte[256];
		}

		this.readPos = 0;
		this.limit = 0;
		this.overLimit = false;
		this.end = false;
	}

	/**
	 * Checks whether the current split is at its end.
	 * 
	 * @return True, if the split is at its end, false otherwise.
	 */
	@Override
	public boolean reachedEnd() {
		return this.end;
	}
	
	@Override
	public OT nextRecord(OT record) throws IOException {
		if (readLine()) {
			return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
		} else {
			this.end = true;
			return null;
		}
	}

	/**
	 * Closes the input by releasing all buffers and closing the file input stream.
	 * 
	 * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
	 */
	@Override
	public void close() throws IOException {
		this.wrapBuffer = null;
		this.readBuffer = null;
		super.close();
	}

	// --------------------------------------------------------------------------------------------

	protected final boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		// position of matching positions in the delimiter byte array
		int delimPos = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				// readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
				if (!fillBuffer(delimPos)) {
					int countInReadBuffer = delimPos;
					if (countInWrapBuffer + countInReadBuffer > 0) {
						// we have bytes left to emit
						if (countInReadBuffer > 0) {
							// we have bytes left in the readBuffer. Move them into the wrapBuffer
							if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
								// reallocate
								byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
								System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
								this.wrapBuffer = tmp;
							}

							// copy readBuffer bytes to wrapBuffer
							System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, countInReadBuffer);
							countInWrapBuffer += countInReadBuffer;
						}

						this.offset += countInWrapBuffer;
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos - delimPos;
			int count;

			// Search for next occurrence of delimiter in read buffer.
			while (this.readPos < this.limit && delimPos < this.delimiter.length) {
				if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
					// Found the expected delimiter character. Continue looking for the next character of delimiter.
					delimPos++;
				} else {
					// Delimiter does not match.
					// We have to reset the read position to the character after the first matching character
					//   and search for the whole delimiter again.
					readPos -= delimPos;
					delimPos = 0;
				}
				readPos++;
			}

			// check why we dropped out
			if (delimPos == this.delimiter.length) {
				// we found a delimiter
				int readBufferBytesRead = this.readPos - startPos;
				this.offset += countInWrapBuffer + readBufferBytesRead;
				count = readBufferBytesRead - this.delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					// check wrap buffer size
					if (this.wrapBuffer.length < countInWrapBuffer + count) {
						final byte[] nb = new byte[countInWrapBuffer + count];
						System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
						this.wrapBuffer = nb;
					}
					if (count >= 0) {
						System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
					}
					setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
					return true;
				} else {
					setResult(this.readBuffer, startPos, count);
					return true;
				}
			} else {
				// we reached the end of the readBuffer
				count = this.limit - startPos;
				
				// check against the maximum record length
				if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {
					throw new IOException("The record length exceeded the maximum record length (" + 
							this.lineLengthLimit + ").");
				}

				// Compute number of bytes to move to wrapBuffer
				// Chars of partially read delimiter must remain in the readBuffer. We might need to go back.
				int bytesToMove = count - delimPos;
				// ensure wrapBuffer is large enough
				if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
					// reallocate
					byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + bytesToMove)];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				// copy readBuffer to wrapBuffer (except delimiter chars)
				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, bytesToMove);
				countInWrapBuffer += bytesToMove;
				// move delimiter chars to the beginning of the readBuffer
				System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

			}
		}
	}
	
	private void setResult(byte[] buffer, int offset, int len) {
		this.currBuffer = buffer;
		this.currOffset = offset;
		this.currLen = len;
	}

	/**
	 * Fills the read buffer with bytes read from the file starting from an offset.
	 */
	private boolean fillBuffer(int offset) throws IOException {
		int maxReadLength = this.readBuffer.length - offset;
		// special case for reading the whole split.
		if (this.splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
			int read = this.stream.read(this.readBuffer, offset, maxReadLength);
			if (read == -1) {
				this.stream.close();
				this.stream = null;
				return false;
			} else {
				this.readPos = offset;
				this.limit = read;
				return true;
			}
		}
		
		// else ..
		int toRead;
		if (this.splitLength > 0) {
			// if we have more data, read that
			toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
		}
		else {
			// if we have exhausted our split, we need to complete the current record, or read one
			// more across the next split.
			// the reason is that the next split will skip over the beginning until it finds the first
			// delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
			// previous split.
			toRead = maxReadLength;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, offset, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = offset; // position from where to start reading
			this.limit = read + offset; // number of valid bytes in the read buffer
			return true;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Config Keys for Parametrization via configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * The configuration key to set the record delimiter.
	 */
	protected static final String RECORD_DELIMITER = "delimited-format.delimiter";
	
	/**
	 * The configuration key to set the number of samples to take for the statistics.
	 */
	private static final String NUM_STATISTICS_SAMPLES = "delimited-format.numSamples";

	// --------------------------------------------------------------------------------------------
	//  Checkpointing
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	@Override
	public Long getCurrentState() throws IOException {
		return this.offset;
	}

	@PublicEvolving
	@Override
	public void reopen(FileInputSplit split, Long state) throws IOException {
		Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
		Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");
		Preconditions.checkArgument(state == -1 || state >= split.getStart(),
			" Illegal offset "+ state +", smaller than the splits start=" + split.getStart());

		try {
			this.open(split);
		} finally {
			this.offset = state;
		}

		if (state > this.splitStart + split.getLength()) {
			this.end = true;
		} else if (state > split.getStart()) {
			initBuffers();

			this.stream.seek(this.offset);
			if (split.getLength() == -1) {
				// this is the case for unsplittable files
				fillBuffer(0);
			} else {
				this.splitLength = this.splitStart + split.getLength() - this.offset;
				if (splitLength <= 0) {
					this.end = true;
				}
			}
		}
	}
}
