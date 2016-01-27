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

package org.apache.flink.runtime.fs.hdfs;

import java.io.IOException;

import org.apache.flink.core.fs.FSDataInputStream;

/**
 * Concrete implementation of the {@link FSDataInputStream} for the
 * Hadoop Distributed File System.
 */
public final class HadoopDataInputStream extends FSDataInputStream {

	private final org.apache.hadoop.fs.FSDataInputStream fsDataInputStream;

	/**
	 * Creates a new data input stream from the given HDFS input stream
	 * 
	 * @param fsDataInputStream
	 *        the HDFS input stream
	 */
	public HadoopDataInputStream(org.apache.hadoop.fs.FSDataInputStream fsDataInputStream) {
		if (fsDataInputStream == null) {
			throw new NullPointerException();
		}
		this.fsDataInputStream = fsDataInputStream;
	}


	@Override
	public synchronized void seek(long desired) throws IOException {
		fsDataInputStream.seek(desired);
	}

	@Override
	public long getPos() throws IOException {
		return fsDataInputStream.getPos();
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
	
	@Override
	public int available() throws IOException {
		return fsDataInputStream.available();
	}

	@Override
	public long skip(long n) throws IOException {
		return fsDataInputStream.skip(n);
	}

	/**
	 * Gets the wrapped Hadoop input stream.
	 * @return The wrapped Hadoop input stream.
	 */
	public org.apache.hadoop.fs.FSDataInputStream getHadoopInputStream() {
		return fsDataInputStream;
	}
}
