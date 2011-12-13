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

package eu.stratosphere.pact.common.io.input;

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.io.statistics.FileBaseStatistics;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Base implementation for delimiter based input formats. By default it splits
 * by line breaks. The key/value pair generation is done in the readLine function
 * which needs to be implemented for specific formats.
 * 
 * @author Moritz Kaufmann
 * @param <K>
 * @param <V>
 */
public abstract class TextInputFormat<K extends Key, V extends Value> extends FileInputFormat<K, V>
{
	/**
	 * The configuration key to set the record delimiter.
	 */
	public static final String RECORD_DELIMITER = "textformat.delimiter";
	
	/**
	 * The configuration key to set the number of samples to take for the statistics.
	 */
	public static final String NUM_STATISTICS_SAMPLES = "textformat.numSamples";
	
	/**
	 * The log.
	 */
	private static final Log LOG = LogFactory.getLog(TextInputFormat.class);
	
	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * The default number of sample lines to consider when calculating the line width.
	 */
	private static final int DEFAULT_NUM_SAMPLES = 10;
	
	// --------------------------------------------------------------------------------------------

	private Class<K> keyClass;
	
	private Class<V> valueClass;
	
	private byte[] readBuffer;

	private byte[] wrapBuffer;

	private int readPos;

	private int limit;

	private byte[] delimiter = new byte[] { '\n' };

	private boolean overLimit;

	private boolean end;
	
	private int bufferSize = -1;
	
	private int numLineSamples;										// the number of lines to sample for statistics

	// --------------------------------------------------------------------------------------------

	protected TextInputFormat()
	{
		this.keyClass = getTemplateType1(getClass());
		this.valueClass = getTemplateType2(getClass());
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This function parses the given byte array which represents a serialized key/value
	 * pair. The parsed content is then returned by setting the pair variables. If the
	 * byte array contains invalid content the record can be skipped by returning <tt>false</tt>.
	 * 
	 * @param pair
	 *        the holder for the key/value pair that is read
	 * @param record
	 *        the serialized key/value pair
	 * @return returns whether the record was successfully deserialized
	 */
	public abstract boolean readLine(KeyValuePair<K, V> pair, byte[] record);

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the delimiter that defines the record boundaries.
	 * 
	 * @return The delimiter, as bytes.
	 */
	public byte[] getDelimiter()
	{
		return delimiter;
	}
	
	/**
	 * Sets the size of the buffer to be used to find record boundaries. This method has only an effect, if it is called
	 * before the input format is opened.
	 * 
	 * @param bufferSize The buffer size to use.
	 */
	public void setBufferSize(int bufferSize)
	{
		this.bufferSize = bufferSize;
	}
	
	/**
	 * Gets the size of the buffer internally used to parse record boundaries.
	 * 
	 * @return The size of the parsing buffer.
	 */
	public int getBufferSize()
	{
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
	public void configure(Configuration parameters)
	{
		super.configure(parameters);
		
		String delimString = parameters.getString(RECORD_DELIMITER, "\n");
		if (delimString == null) {
			throw new IllegalArgumentException("The delimiter not be null.");
		}

		this.delimiter = delimString.getBytes();
		
		// set the number of samples
		this.numLineSamples = DEFAULT_NUM_SAMPLES;
		String samplesString = parameters.getString(NUM_STATISTICS_SAMPLES, null);
		
		if (samplesString != null) {
			try {
				this.numLineSamples = Integer.parseInt(samplesString);
			}
			catch (NumberFormatException nfex) {
				if (LOG.isWarnEnabled())
					LOG.warn("Invalid value for number of samples to take: " + samplesString +
							". Using default value of " + DEFAULT_NUM_SAMPLES);
			}
		}
	}
	
	
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics)
	{
		// check the cache
		FileBaseStatistics stats = null;
		
		if (cachedStatistics != null && cachedStatistics instanceof FileBaseStatistics) {
			stats = (FileBaseStatistics) cachedStatistics;
		}
		else {
			stats = new FileBaseStatistics(-1, BaseStatistics.UNKNOWN, BaseStatistics.UNKNOWN);
		}
		

		try {
			final Path file = this.filePath;
			final URI uri = file.toUri();

			// get the filesystem
			final FileSystem fs = FileSystem.get(uri);
			List<FileStatus> files = null;

			// get the file info and check whether the cached statistics are still
			// valid.
			{
				FileStatus status = fs.getFileStatus(file);

				if (status.isDir()) {
					FileStatus[] fss = fs.listStatus(file);
					files = new ArrayList<FileStatus>(fss.length);
					boolean unmodified = true;

					for (FileStatus s : fss) {
						if (!s.isDir()) {
							files.add(s);
							if (s.getModificationTime() > stats.getLastModificationTime()) {
								stats.setFileModTime(s.getModificationTime());
								unmodified = false;
							}
						}
					}

					if (unmodified) {
						return stats;
					}
				}
				else {
					// check if the statistics are up to date
					long modTime = status.getModificationTime();	
					if (stats.getLastModificationTime() == modTime) {
						return stats;
					}

					stats.setFileModTime(modTime);
					
					files = new ArrayList<FileStatus>(1);
					files.add(status);
				}
			}

			stats.setAvgBytesPerRecord(-1.0f);
			stats.setFileSize(0);
			
			// calculate the whole length
			for (FileStatus s : files) {
				stats.setFileSize(s.getLen());
			}
			
			// sanity check
			if (stats.getTotalInputSize() <= 0) {
				stats.setFileSize(BaseStatistics.UNKNOWN);
				return stats;
			}
			

			// currently, the sampling only works on line separated data
			final byte[] delimiter = getDelimiter();
			if (! ((delimiter.length == 1 && delimiter[0] == '\n') ||
				   (delimiter.length == 2 && delimiter[0] == '\r' && delimiter[1] == '\n')) )
			{
				return stats;
			}
						
			// make the samples small for very small files
			int numSamples = Math.min(this.numLineSamples, (int) (stats.getTotalInputSize() / 1024));
			if (numSamples < 2) {
				numSamples = 2;
			}

			long offset = 0;
			long bytes = 0; // one byte for the line-break
			long stepSize = stats.getTotalInputSize() / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			for (int sampleNum = 0; sampleNum < numSamples && fileNum < files.size(); sampleNum++) {
				FileStatus currentFile = files.get(fileNum);
				FSDataInputStream inStream = null;

				try {
					inStream = fs.open(currentFile.getPath());
					LineReader lineReader = new LineReader(inStream, offset, currentFile.getLen() - offset, 1024);
					byte[] line = lineReader.readLine();
					lineReader.close();

					if (line != null && line.length > 0) {
						samplesTaken++;
						bytes += line.length + 1; // one for the linebreak
					}
				}
				finally {
					// make a best effort to close
					if (inStream != null) {
						try {
							inStream.close();
						} catch (Throwable t) {}
					}
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < files.size() && offset >= (currentFile = files.get(fileNum)).getLen()) {
					offset -= currentFile.getLen();
					fileNum++;
				}
			}

			stats.setAvgBytesPerRecord(bytes / (float) samplesTaken);
		}
		catch (IOException ioex) {
			if (LOG.isWarnEnabled())
				LOG.warn("Could not determine complete statistics for file '" + filePath + "' due to an io error: "
						+ ioex.getMessage());
		}
		catch (Throwable t) {
			if (LOG.isErrorEnabled())
				LOG.error("Unexpected problen while getting the file statistics for file '" + filePath + "': "
						+ t.getMessage(), t);
		}

		return stats;
	}

	/**
	 * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
	 * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
	 * 
	 * @param split The input split to open.
	 * 
	 * @see eu.stratosphere.pact.common.io.input.FileInputFormat#open(eu.stratosphere.nephele.fs.FileInputSplit)
	 */
	@Override
	public void open(FileInputSplit split) throws IOException
	{
		super.open(split);
		
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;
		this.readBuffer = new byte[this.bufferSize];
		this.wrapBuffer = new byte[256];

		this.readPos = 0;
		this.overLimit = false;
		this.end = false;

		if (this.start != 0) {
			this.stream.seek(this.start);
			readLine();
			
			// if the first partial record already pushes the stream over the limit of our split, then no
			// record starts within this split 
			if (this.overLimit) {
				this.end = true;
			}
		}
		else {
			fillBuffer();
		}
	}

	/**
	 * Checks whether the current split is at its end.
	 * 
	 * @return True, if the split is at its end, false otherwise.
	 */
	@Override
	public boolean reachedEnd()
	{
		return this.end;
	}
	
	@Override
	public boolean nextRecord(KeyValuePair<K, V> pair) throws IOException
	{
		byte[] line = readLine();
		if (line == null) {
			this.end = true;
			return false;
		} else {
			return readLine(pair, line);
		}
	}

	/**
	 * Closes the input by releasing all buffers and closing the file input stream.
	 * 
	 * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
	 */
	@Override
	public void close() throws IOException
	{
		this.wrapBuffer = null;
		this.readBuffer = null;
		
		super.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public KeyValuePair<K, V> createPair()
	{
		try {
			return new KeyValuePair<K, V>(this.keyClass.newInstance(), this.valueClass.newInstance());
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	// --------------------------------------------------------------------------------------------

	private byte[] readLine() throws IOException {
		if (this.stream == null || this.overLimit) {
			return null;
		}

		int countInWrapBuffer = 0;

		/* position of matching positions in the delimiter byte array */
		int i = 0;

		while (true) {
			if (this.readPos >= this.limit) {
				if (!fillBuffer()) {
					if (countInWrapBuffer > 0) {
						byte[] tmp = new byte[countInWrapBuffer];
						System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
						return tmp;
					} else {
						return null;
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
					byte[] end = new byte[countInWrapBuffer + count];
					if (count >= 0) {
						System.arraycopy(this.wrapBuffer, 0, end, 0, countInWrapBuffer);
						System.arraycopy(this.readBuffer, 0, end, countInWrapBuffer, count);
						return end;
					} else {
						// count < 0
						System.arraycopy(this.wrapBuffer, 0, end, 0, countInWrapBuffer + count);
						return end;
					}
				} else {
					byte[] end = new byte[count];
					System.arraycopy(this.readBuffer, startPos, end, 0, count);
					return end;
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

	private final boolean fillBuffer() throws IOException {
		int toRead = this.length > this.readBuffer.length ? this.readBuffer.length : (int) this.length;
		if (this.length <= 0) {
			toRead = this.readBuffer.length;
			this.overLimit = true;
		}

		int read = this.stream.read(this.readBuffer, 0, toRead);

		if (read == -1) {
			this.stream.close();
			this.stream = null;
			return false;
		} else {
			this.length -= read;
			this.readPos = 0;
			this.limit = read;
			return true;
		}
	}
}
