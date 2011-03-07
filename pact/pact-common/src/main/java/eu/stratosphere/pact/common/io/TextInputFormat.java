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

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
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
public abstract class TextInputFormat<K extends Key, V extends Value> extends InputFormat<K, V> {

	public static final String FORMAT_PAIR_DELIMITER = "delimiter";

	private byte[] readBuffer;

	private byte[] wrapBuffer;

	private int readPos;

	private int limit;

	private byte[] delimiter = new byte[] { '\n' };

	private boolean overLimit;

	private boolean end;

	private static final Log LOG = LogFactory.getLog(TextInputFormat.class);

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

	@Override
	public boolean nextPair(KeyValuePair<K, V> pair) {
		// TODO: Check whether it is closed or was openend
		try {
			byte[] line = readLine();
			if (line == null) {
				end = true;
				return false;
			} else {
				return readLine(pair, line);
			}
		} catch (IOException e) {
			LOG.error(e);
			return false;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters) {
		String delimString = parameters.getString(FORMAT_PAIR_DELIMITER, "\n");

		if (delimString == null) {
			throw new IllegalArgumentException("The delimiter not be null.");
		}

		delimiter = delimString.getBytes();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IOException
	 */
	@Override
	public void open() throws IOException {
		readBuffer = new byte[bufferSize];
		wrapBuffer = new byte[256];

		this.readPos = 0;
		this.overLimit = false;
		this.end = false;

		if (start != 0) {
			stream.seek(start);
			readLine();
		} else {
			fillBuffer();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean reachedEnd() {
		return end;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		wrapBuffer = null;
		readBuffer = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public KeyValuePair<K, V> createPair() {
		try {
			return new KeyValuePair<K, V>(ok.newInstance(), ov.newInstance());
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initTypes() {
		super.ok = getTemplateType1(getClass());
		super.ov = getTemplateType2(getClass());
	}

	private byte[] readLine() throws IOException {
		if (stream == null || overLimit) {
			return null;
		}

		int countInWrapBuffer = 0;

		/* position of matching positions in the delimiter byte array */
		int i = 0;

		while (true) {
			if (readPos >= limit) {
				if (!fillBuffer()) {
					if (countInWrapBuffer > 0) {
						byte[] tmp = new byte[countInWrapBuffer];
						System.arraycopy(wrapBuffer, 0, tmp, 0, countInWrapBuffer);
						return tmp;
					} else {
						return null;
					}
				}
			}

			int startPos = readPos;
			int count = 0;

			while (readPos < limit && i < delimiter.length) {
				if ((readBuffer[readPos++]) == delimiter[i]) {
					i++;
				} else {
					i = 0;
				}

			}

			// check why we dropped out
			if (i == delimiter.length) {
				// line end
				count = readPos - startPos - delimiter.length;

				// copy to byte array
				if (countInWrapBuffer > 0) {
					byte[] end = new byte[countInWrapBuffer + count];
					if (count >= 0) {
						System.arraycopy(wrapBuffer, 0, end, 0, countInWrapBuffer);
						System.arraycopy(readBuffer, 0, end, countInWrapBuffer, count);
						return end;
					} else {
						// count < 0
						System.arraycopy(wrapBuffer, 0, end, 0, countInWrapBuffer + count);
						return end;
					}
				} else {
					byte[] end = new byte[count];
					System.arraycopy(readBuffer, startPos, end, 0, count);
					return end;
				}
			} else {
				count = limit - startPos;

				// buffer exhausted
				while (wrapBuffer.length - countInWrapBuffer < count) {
					// reallocate
					byte[] tmp = new byte[wrapBuffer.length * 2];
					System.arraycopy(wrapBuffer, 0, tmp, 0, countInWrapBuffer);
					wrapBuffer = tmp;
				}

				System.arraycopy(readBuffer, startPos, wrapBuffer, countInWrapBuffer, count);
				countInWrapBuffer += count;
			}
		}
	}

	private final boolean fillBuffer() throws IOException {
		int toRead = length > readBuffer.length ? readBuffer.length : (int) length;
		if (length <= 0) {
			toRead = readBuffer.length;
			overLimit = true;
		}

		int read = stream.read(readBuffer, 0, toRead);

		if (read == -1) {
			stream.close();
			stream = null;
			return false;
		} else {
			length -= read;
			readPos = 0;
			limit = read;
			return true;
		}

	}

	public byte[] getDelimiter() {
		return delimiter;
	}
}
