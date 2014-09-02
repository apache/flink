/**
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


package org.apache.flink.runtime.testutils.tasks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.StringRecord;
import org.apache.flink.runtime.io.network.api.RecordReader;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

/**
 * A file line writer reads string records its input gate and writes them to the associated output file.
 * 
 */
public class FileLineWriter extends AbstractInvokable {
	/**
	 * The record reader through which incoming string records are received.
	 */
	private RecordReader<StringRecord> input = null;


	@Override
	public void invoke() throws Exception {

		final Configuration conf = getEnvironment().getTaskConfiguration();
		final String outputPathString = conf.getString(JobFileOutputVertex.PATH_PROPERTY, null);
		
		Path outputPath = new Path(outputPathString);

		FileSystem fs = FileSystem.get(outputPath.toUri());
		if (fs.exists(outputPath)) {
			FileStatus status = fs.getFileStatus(outputPath);

			if (status.isDir()) {
				outputPath = new Path(outputPath.toUri().toString() + "/file_" + getIndexInSubtaskGroup() + ".txt");
			}
		}

		final FSDataOutputStream outputStream = fs.create(outputPath, true);

		while (this.input.hasNext()) {

			StringRecord record = this.input.next();
			byte[] recordByte = (record.toString() + "\r\n").getBytes();
			outputStream.write(recordByte, 0, recordByte.length);
		}

		outputStream.close();

	}

	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<StringRecord>(this, StringRecord.class);
	}
}