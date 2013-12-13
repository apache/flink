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

package eu.stratosphere.nephele.fs.hdfs;

import java.io.IOException;

import eu.stratosphere.nephele.fs.FSDataInputStream;

/**
 * Concrete implementation of the {@link FSDataInputStream} for the
 * Hadoop Distributed File System.
 * 
 * @author warneke
 */
public final class DistributedDataInputStream extends FSDataInputStream {

	private org.apache.hadoop.fs.FSDataInputStream fsDataInputStream = null;

	/**
	 * Creates a new data input stream from the given HDFS input stream
	 * 
	 * @param fsDataInputStream
	 *        the HDFS input stream
	 */
	public DistributedDataInputStream(org.apache.hadoop.fs.FSDataInputStream fsDataInputStream) {
		this.fsDataInputStream = fsDataInputStream;
	}

	@Override
	public synchronized void seek(long desired) throws IOException {

		fsDataInputStream.seek(desired);
	}

	@Override
	public int read() throws IOException {

		return fsDataInputStream.read();
	}

	@Override
	public void close() throws IOException {

		fsDataInputStream.close();
	}

	@Override
	public int read(byte[] buffer, int offset, int length) throws IOException {

		return fsDataInputStream.read(buffer, offset, length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() throws IOException {
		return fsDataInputStream.available();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(long n) throws IOException {
		return fsDataInputStream.skip(n);
	}

}
