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

package org.apache.flink.formats.thrift.utils;

import org.apache.flink.formats.thrift.typeutils.ThriftSerializer;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * ThriftWriter handles writing records to files in thrift format.
 */
public class ThriftWriter {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializer.class);

	/**
	 * File to write to.
	 */
	protected final File file;

	/**
	 * For writing to the file.
	 */
	private BufferedOutputStream bufferedOut;

	/**
	 * For serialization of objects.
	 */
	private Class<? extends TProtocol> tProtocolClazz;

	private TProtocol binaryOut;

	/**
	 * Constructor.
	 */
	public ThriftWriter(File file, Class<? extends TProtocol> tProtocolClazz) {
		this.file = file;
		this.tProtocolClazz = tProtocolClazz;
	}

	/**
	 * Open the file for writing.
	 */
	public void open() throws IOException {
		try {
			this.bufferedOut = new BufferedOutputStream(new FileOutputStream(file), 2048);
			TIOStreamTransport iostreamTransport = new TIOStreamTransport(bufferedOut);
			this.binaryOut = this.tProtocolClazz.getConstructor(TTransport.class).newInstance(iostreamTransport);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * Write the object to disk.
	 */
	public void write(TBase t) throws IOException {
		try {
			t.write(binaryOut);
			bufferedOut.flush();
		} catch (TException e) {
			throw new IOException(e);
		}
	}

	public void write(byte[] bytes) throws IOException {
		bufferedOut.write(bytes);
		bufferedOut.flush();
	}

	/**
	 * Close the file stream.
	 */
	public void close() throws IOException {
		bufferedOut.close();
	}
}
