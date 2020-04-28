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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.net.URI;

/**
 * A {@link FileInputFormat} that wraps a {@link HiveTableInputFormat}.
 */
public class HiveTableFileInputFormat extends FileInputFormat<RowData> {

	private HiveTableInputFormat inputFormat;
	private HiveTablePartition hiveTablePartition;

	public HiveTableFileInputFormat(
			HiveTableInputFormat inputFormat,
			HiveTablePartition hiveTablePartition) {
		this.inputFormat = inputFormat;
		this.hiveTablePartition = hiveTablePartition;
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		URI uri = fileSplit.getPath().toUri();
		HiveTableInputSplit split = new HiveTableInputSplit(
				fileSplit.getSplitNumber(),
				new FileSplit(new Path(uri), fileSplit.getStart(), fileSplit.getLength(), (String[]) null),
				inputFormat.getJobConf(),
				hiveTablePartition
		);
		inputFormat.open(split);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return inputFormat.reachedEnd();
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		return inputFormat.nextRecord(reuse);
	}

	@Override
	public void configure(Configuration parameters) {
		inputFormat.configure(parameters);
		super.configure(parameters);
	}

	@Override
	public void close() throws IOException {
		inputFormat.close();
		super.close();
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		inputFormat.setRuntimeContext(t);
		super.setRuntimeContext(t);
	}

	@Override
	public void openInputFormat() throws IOException {
		inputFormat.openInputFormat();
		super.openInputFormat();
	}

	@Override
	public void closeInputFormat() throws IOException {
		inputFormat.closeInputFormat();
		super.closeInputFormat();
	}
}
