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


package org.apache.flink.api.avro;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.SeekableInput;
import org.apache.flink.core.fs.FSDataInputStream;


/**
 * Code copy pasted from org.apache.avro.mapred.FSInput (which is Apache licensed as well)
 * 
 * The wrapper keeps track of the position in the data stream.
 */
public class FSDataInputStreamWrapper implements Closeable, SeekableInput {
	private final FSDataInputStream stream;
	private long pos;
	private long len;

	public FSDataInputStreamWrapper(FSDataInputStream stream, long len) {
		this.stream = stream;
		this.pos = 0;
		this.len = len;
	}

	public long length() throws IOException {
		return this.len;
	}

	public int read(byte[] b, int off, int len) throws IOException {
		int read;
		read = stream.read(b, off, len);
		pos += read;
		return read;
	}

	public void seek(long p) throws IOException {
		stream.seek(p);
		pos = p;
	}

	public long tell() throws IOException {
		return pos;
	}

	public void close() throws IOException {
		stream.close();
	}
}
