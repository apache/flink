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

package eu.stratosphere.nephele.fs.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import eu.stratosphere.nephele.fs.FSDataInputStream;

/**
 * The <code>LocalDataInputStream</code> class is a wrapper class for a data
 * input stream to the local file system.
 * 
 * @author warneke
 */
public class LocalDataInputStream extends FSDataInputStream {

	/**
	 * The file input stream used to read data.
	 */
	private FileInputStream fis = null;

	/**
	 * The current position in the stream.
	 */
	private long position = 0;

	/**
	 * Constructs a new <code>LocalDataInputStream</code> object from a given {@link File} object.
	 * 
	 * @param file
	 *        the {@link File} object the data stream is written to
	 * @throws IOException
	 *         thrown if the data input stream cannot be created
	 */
	public LocalDataInputStream(File file)
											throws IOException {

		fis = new FileInputStream(file);
		position = 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(long desired) throws IOException {

		fis.getChannel().position(desired);
		position = desired;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {

		final int value = fis.read();
		if (value >= 0) {
			this.position++;
		}

		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(byte[] buffer, int offset, int length) throws IOException {

		final int value = fis.read(buffer, offset, length);
		if (value > 0) {
			this.position += value;
		}

		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		fis.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() throws IOException {
		return fis.available();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(long n) throws IOException {
		return fis.skip(n);
	}

}
