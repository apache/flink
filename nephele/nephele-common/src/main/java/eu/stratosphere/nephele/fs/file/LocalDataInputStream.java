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
	public LocalDataInputStream(final File file) throws IOException {

		this.fis = new FileInputStream(file);
		this.position = 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(final long desired) throws IOException {

		this.fis.getChannel().position(desired);
		this.position = desired;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {

		final int value = this.fis.read();
		if (value >= 0) {
			this.position++;
		}

		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final byte[] buffer, final int offset, final int length) throws IOException {

		final int value = this.fis.read(buffer, offset, length);
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

		this.fis.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() throws IOException {
		return this.fis.available();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(final long n) throws IOException {
		return this.fis.skip(n);
	}

}
