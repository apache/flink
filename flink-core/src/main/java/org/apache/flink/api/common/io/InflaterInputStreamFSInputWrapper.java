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

import java.io.IOException;
import java.util.zip.InflaterInputStream;

import org.apache.flink.core.fs.FSDataInputStream;

public class InflaterInputStreamFSInputWrapper extends FSDataInputStream {

	private InflaterInputStream inStream;

	public InflaterInputStreamFSInputWrapper(FSDataInputStream inStream) {
		this.inStream = new InflaterInputStream(inStream);
	}
	
	@Override
	public void seek(long desired) throws IOException {
		throw new UnsupportedOperationException("Compressed streams do not support the seek operation");
	}

	@Override
	public long getPos() throws IOException {
		throw new UnsupportedOperationException("Compressed streams do not support the getPos operation");
	}

	@Override
	public int read() throws IOException {
		return inStream.read();
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return inStream.read(b, off, len);
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return inStream.read(b);
	}
}
