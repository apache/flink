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

package eu.stratosphere.pact.common.recordio;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Base implementation for delimiter based input formats. By default it splits
 * by line breaks. The key/value pair generation is done in the readLine function
 * which needs to be implemented for specific formats.
 * 
 * @author Moritz Kaufmann
 * @author Stephan Ewen
 */
public abstract class TextInputFormat extends FileInputFormat
{	
	public static final String FORMAT_PAIR_DELIMITER = "delimiter";

	private byte[] readBuffer;

	private byte[] wrapBuffer;

	private int readPos;

	private int limit;

	private byte[] delimiter = new byte[] { '\n' };

	private boolean overLimit;

	private boolean end;

	// --------------------------------------------------------------------------------------------

	/**
	 * This function parses the given byte array which represents a serialized key/value
	 * pair. The parsed content is then returned by setting the pair variables. If the
	 * byte array contains invalid content the record can be skipped by returning <tt>false</tt>.
	 * 
	 * @param record The target record into which deserialized data is put.
	 * @param line The serialized binary data.
	 * @return returns True, if the record was successfully deserialized, false otherwise.
	 */
	public abstract boolean readLine(PactRecord record, byte[] line);

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordio.InputFormat#nextRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public boolean nextRecord(PactRecord record) throws IOException
	{
		byte[] line = readLine();
		if (line == null) {
			this.end = true;
			return false;
		}
		else {
			return readLine(record, line);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters) {
		String delimString = parameters.getString(FORMAT_PAIR_DELIMITER, "\n");

		if (delimString == null) {
			throw new IllegalArgumentException("The delimiter must not be null.");
		}

		this.delimiter = delimString.getBytes();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IOException
	 */
	@Override
	public void open(InputSplit split) throws IOException
	{
		super.open(split);
		
		this.readBuffer = new byte[this.bufferSize];
		this.wrapBuffer = new byte[256];

		this.readPos = 0;
		this.overLimit = false;
		this.end = false;

		if (this.start != 0) {
			this.stream.seek(this.start);
			readLine();
		}
		else {
			fillBuffer();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean reachedEnd() {
		return this.end;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException
	{
		super.close();
		this.wrapBuffer = null;
		this.readBuffer = null;
	}
	
	public byte[] getDelimiter() {
		return delimiter;
	}
	
	

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
					wrapBuffer = tmp;
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
		}
		else {
			this.length -= read;
			this.readPos = 0;
			this.limit = read;
			return true;
		}

	}
}
