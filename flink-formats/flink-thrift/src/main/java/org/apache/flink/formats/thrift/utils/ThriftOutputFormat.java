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

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;

import java.io.File;
import java.io.IOException;

/**
 * ThriftOutputFormat.
 */
public class ThriftOutputFormat<T extends TBase, TP extends TProtocol> extends FileOutputFormat<T> {

	private transient ThriftWriter thriftWriter;
	private Class<TP> tprotocolClass;

	public ThriftOutputFormat(Path filePath, Class<TP> tprotocolClass) {
		super(filePath);
		this.tprotocolClass = tprotocolClass;
	}

	@Override
	protected String getDirectoryFileName(int taskNumber) {
		return super.getDirectoryFileName(taskNumber) + ".thrift";
	}

	@Override
	public void writeRecord(T record) throws IOException {
		thriftWriter.write(record);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		File file = new File(this.outputFilePath.getPath());
		this.thriftWriter = new ThriftWriter(file, tprotocolClass);
		thriftWriter.open();
	}

	@Override
	public void close() throws IOException {
		thriftWriter.close();
		super.close();
	}
}
