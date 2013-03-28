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

package eu.stratosphere.pact.generic.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.util.PactConfigConstants;

/**
 * Base implementation for delimiter based input formats.
 */
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT> {
	
	// -------------------------------------- Constants -------------------------------------------
	
	/**
	 * The log.
	 */
	private static final Log LOG = LogFactory.getLog(DelimitedInputFormat.class);
	
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
	private static final int DEFAULT_MAX_NUM_SAMPLES;
	
	/**
	 * The minimum number of line samples to be taken.
	 */
	private static final int DEFAULT_MIN_NUM_SAMPLES;
	
	static {
		int maxSamples = GlobalConfiguration.getInteger(PactConfigConstants.DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY,
				PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES);
		int minSamples = GlobalConfiguration.getInteger(PactConfigConstants.DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY,
			PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES);
		
		if (maxSamples < 0) {
			LOG.error("Invalid default maximum number of line samples: " + maxSamples + ". Using default value of " +
				PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES);
			maxSamples = PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES;
		}
		if (minSamples < 0) {
			LOG.error("Invalid default minimum number of line samples: " + minSamples + ". Using default value of " +
				PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES);
			minSamples = PactConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES;
		}
		
		DEFAULT_MAX_NUM_SAMPLES = maxSamples;
		
		if (minSamples > maxSamples) {
			LOG.error("Defaul minimum number of line samples cannot be greater the default maximum number " +
					"of line samples: min=" + minSamples + ", max=" + maxSamples + ". Defaulting minumum to maximum.");
			DEFAULT_MIN_NUM_SAMPLES = maxSamples;
		} else {
			DEFAULT_MIN_NUM_SAMPLES = minSamples;
		}
	}
	
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	/**
	 * The configuration key to set the record delimiter.
	 */
	public static final String RECORD_DELIMITER = "delimited-format.delimiter";
	
	/**
	 * The configuration key to set the record delimiter encoding.
	 */
	private static final String RECORD_DELIMITER_ENCODING = "delimited-format.delimiter-encoding";
	
	/**
	 * The configuration key to set the number of samples to take for the statistics.
	 */
	private static final String NUM_STATISTICS_SAMPLES = "delimited-format.numSamples";
	
	// --------------------------------------------------------------------------------------------
	
	protected byte[] readBuffer;

	protected byte[] wrapBuffer;

	protected int readPos;

	protected int limit;

	protected byte[] delimiter = new byte[] {'\n'};
	
	private byte[] currBuffer;
	private int currOffset;
	private int currLen;

	protected boolean overLimit;

	protected boolean end;
	
	protected int bufferSize = -1;
	
	protected int numLineSamples;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This function parses the given byte array which represents a serialized key/value
	 * pair. The parsed content is then returned by setting the pair variables. If the
	 * byte array contains invalid content the record can be skipped by returning <tt>false</tt>.
	 * 
	 * @param record The holder for the line that is read.
	 * @param bytes The serialized record.
	 * @return returns whether the record was successfully deserialized
	 */
	public abstract boolean readRecord(OT target, byte[] bytes, int offset, int numBytes);

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the delimiter that defines the record boundaries.
	 * 
	 * @return The delimiter, as bytes.
	 */
	public byte[] getDelimiter() {
		return this.delimiter;
	}
	
	/**
	 * Sets the size of the buffer to be used to find record boundaries. This method has only an effect, if it is called
	 * before the input format is opened.
	 * 
	 * @param bufferSize The buffer size to use.
	 */
	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}
	
	/**
	 * Gets the size of the buffer internally used to parse record boundaries.
	 * 
	 * @return The size of the parsing buffer.
	 */
	public int getBufferSize() {
		return this.readBuffer == null ? 0: this.readBuffer.length;
	}
	
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
		
		final String delimString = parameters.getString(RECORD_DELIMITER, AbstractConfigBuilder.NEWLINE_DELIMITER);
		if (delimString == null) {
			throw new IllegalArgumentException("The delimiter not be null.");
		}
		final String charsetName = parameters.getString(RECORD_DELIMITER_ENCODING, null);

		try {
			this.delimiter = charsetName == null ? delimString.getBytes() : delimString.getBytes(charsetName);
		} catch (UnsupportedEncodingException useex) {
			throw new IllegalArgumentException("The charset with the name '" + charsetName + 
				"' is not supported on this TaskManager instance.", useex);
		}
		
		// set the number of samples
		this.numLineSamples = NUM_SAMPLES_UNDEFINED;
		final String samplesString = parameters.getString(NUM_STATISTICS_SAMPLES, null);
		if (samplesString != null) {
			try {
				this.numLineSamples = Integer.parseInt(samplesString);
				if (this.numLineSamples < 0) {
					throw new NumberFormatException();
				}
			}
			catch (NumberFormatException nfex) {
				if (LOG.isWarnEnabled())
					LOG.warn("Invalid value for number of samples to take: " + samplesString + ". Skipping sampling.");
				this.numLineSamples = 0;
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		
		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
				(FileBaseStatistics) cachedStats : null;
		
		try {
			final Path filePath = this.filePath;
		
			// get the filesystem
			final FileSystem fs = FileSystem.get(filePath.toUri());
			final ArrayList<FileStatus> allFiles = new ArrayList<FileStatus>(1);
			
			// let the file input format deal with the up-to-date check and the basic size
			final FileBaseStatistics stats = getFileStats(cachedFileStats, filePath, fs, allFiles);
			if (stats == null) {
				return null;
			}
			
			// check whether the width per record is already known or the total size is unknown as well
			// in both cases, we return the stats as they are
			if (stats.getAverageRecordWidth() != FileBaseStatistics.AVG_RECORD_BYTES_UNKNOWN ||
					stats.getTotalInputSize() == FileBaseStatistics.SIZE_UNKNOWN) {
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
			
			
			final int delimiterLength = getDelimiter().length;
			
			// set a small read buffer size
			this.bufferSize = 1024;

			long offset = 0;
			long totalNumBytes = 0;
			long stepSize = stats.getTotalInputSize() / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			for (int sampleNum = 0; sampleNum < numSamples && fileNum < allFiles.size(); sampleNum++) {
				// make a split for the sample and use it to read a record
				FileStatus file = allFiles.get(fileNum);
				FileInputSplit split = new FileInputSplit(sampleNum, file.getPath(), offset, file.getLen() - offset, null);

				// we open the split, read one line, and take its length
				try {
					open(split);
					readLine();
					totalNumBytes += this.currLen + delimiterLength;
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
			if (LOG.isWarnEnabled())
				LOG.warn("Could not determine statistics for file '" + this.filePath + "' due to an io error: "
						+ ioex.getMessage());
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled())
				LOG.error("Unexpected problen while getting the file statistics for file '" + this.filePath + "': "
						+ t.getMessage(), t);
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
	 * @see eu.stratosphere.pact.common.io.FileInputFormat#open(eu.stratosphere.nephele.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;
		
		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}
		if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
			this.wrapBuffer = new byte[256];
		}

		this.readPos = 0;
		this.overLimit = false;
		this.end = false;

		if (this.splitStart != 0) {
			this.stream.seek(this.splitStart);
			readLine();
			
			// if the first partial record already pushes the stream over the limit of our split, then no
			// record starts within this split 
			if (this.overLimit) {
				this.end = true;
			}
		} else {
			fillBuffer();
		}
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.io.InputFormat#nextRecord(java.lang.Object)
	 */
	@Override
	public boolean nextRecord(OT record) throws IOException {
		if (readLine()) {
			return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
		} else {
			this.end = true;
			return false;
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

	private boolean readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return false;
		}

		int countInWrapBuffer = 0;

		/* position of matching positions in the delimiter byte array */
		int i = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				if (!fillBuffer()) {
					if (countInWrapBuffer > 0) {
						setResult(this.wrapBuffer, 0, countInWrapBuffer);
						return true;
					} else {
						return false;
					}
				}
			}

			int startPos = this.readPos;
			int count = 0;

			while (this.readPos < this.limit && i < this.delimiter.length) {
				if ((this.readBuffer[this.readPos++]) == this.delimiter[i]) {
					i++;
				} else {
					i = 0;
				}

			}

			// check why we dropped out
			if (i == this.delimiter.length) {
				// line end
				count = this.readPos - startPos - this.delimiter.length;

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
				count = this.limit - startPos;

				// buffer exhausted
				while (this.wrapBuffer.length - countInWrapBuffer < count) {
					// reallocate
					byte[] tmp = new byte[this.wrapBuffer.length * 2];
					System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					this.wrapBuffer = tmp;
				}

				System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, count);
				countInWrapBuffer += count;
			}
		}
	}
	
	private final void setResult(byte[] buffer, int offset, int len) {
		this.currBuffer = buffer;
		this.currOffset = offset;
		this.currLen = len;
	}

	private final boolean fillBuffer() throws IOException {
		int toRead = this.splitLength > this.readBuffer.length ? this.readBuffer.length : (int) this.splitLength;
		if (this.splitLength <= 0) {
			toRead = this.readBuffer.length;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, 0, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.splitLength -= read;
			this.readPos = 0;
			this.limit = read;
			return true;
		}
	}
	
	// ============================================================================================
	
	/**
	 * Creates a configuration builder that can be used to set the input format's parameters to the config in a fluent
	 * fashion.
	 * 
	 * @return A config builder for setting parameters.
	 */
	public static ConfigBuilder configureDelimitedFormat(FileDataSource target) {
		return new ConfigBuilder(target.getParameters());
	}
	
	/**
	 * Abstract builder used to set parameters to the input format's configuration in a fluent way.
	 */
	protected static class AbstractConfigBuilder<T> extends FileInputFormat.AbstractConfigBuilder<T> {
		
		private static final String NEWLINE_DELIMITER = "\n";
		
		// --------------------------------------------------------------------
		
		/**
		 * Creates a new builder for the given configuration.
		 * 
		 * @param targetConfig The configuration into which the parameters will be written.
		 */
		protected AbstractConfigBuilder(Configuration config) {
			super(config);
		}
		
		// --------------------------------------------------------------------
		
		/**
		 * Sets the delimiter to be a single character, namely the given one. The character must be within
		 * the value range <code>0</code> to <code>127</code>.
		 * 
		 * @param delimiter The delimiter character.
		 * @return The builder itself.
		 */
		public T recordDelimiter(char delimiter) {
			if (delimiter == '\n') {
				this.config.setString(RECORD_DELIMITER, NEWLINE_DELIMITER);
			} else {
				this.config.setString(RECORD_DELIMITER, String.valueOf(delimiter));
			}
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter to be the given string. The string will be converted to bytes for more efficient
		 * comparison during input parsing. The conversion will be done using the platforms default charset.
		 * 
		 * @param delimiter The delimiter string.
		 * @return The builder itself.
		 */
		public T recordDelimiter(String delimiter) {
			this.config.setString(RECORD_DELIMITER, delimiter);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the delimiter to be the given string. The string will be converted to bytes for more efficient
		 * comparison during input parsing. The conversion will be done using the charset with the given name.
		 * The charset must be available on the processing nodes, otherwise an exception will be raised at
		 * runtime.
		 * 
		 * @param delimiter The delimiter string.
		 * @param charsetName The name of the encoding character set.
		 * @return The builder itself.
		 */
		public T recordDelimiter(String delimiter, String charsetName) {
			this.config.setString(RECORD_DELIMITER, delimiter);
			this.config.setString(RECORD_DELIMITER_ENCODING, charsetName);
			@SuppressWarnings("unchecked")
			T ret = (T) this;
			return ret;
		}
		
		/**
		 * Sets the number of line samples to take in order to estimate the base statistics for the
		 * input format.
		 * 
		 * @param numSamples The number of line samples to take.
		 * @return The builder itself.
		 */
		public T numSamplesForStatistics(int numSamples) {
			this.config.setInteger(NUM_STATISTICS_SAMPLES, numSamples);
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
