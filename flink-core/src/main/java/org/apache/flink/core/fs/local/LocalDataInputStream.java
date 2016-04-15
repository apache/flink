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


package org.apache.flink.core.fs.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;

/**
 * The <code>LocalDataInputStream</code> class is a wrapper class for a data
 * input stream to the local file system.
 * 
 */
@Internal
public class LocalDataInputStream extends FSDataInputStream {

	/**
	 * The file input stream used to read data.
	 */
	private FileInputStream fis = null;

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
	}


	@Override
	public void seek(final long desired) throws IOException {

		this.fis.getChannel().position(desired);
	}

	@Override
	public long getPos() throws IOException {
		return this.fis.getChannel().position();
	}


	@Override
	public int read() throws IOException {

		return this.fis.read();
	}


	@Override
	public int read(final byte[] buffer, final int offset, final int length) throws IOException {

		return this.fis.read(buffer, offset, length);
	}


	@Override
	public void close() throws IOException {

		this.fis.close();
	}


	@Override
	public int available() throws IOException {
		return this.fis.available();
	}


	@Override
	public long skip(final long n) throws IOException {
		return this.fis.skip(n);
	}

}
