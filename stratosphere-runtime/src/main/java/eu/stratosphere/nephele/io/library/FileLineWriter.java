/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.library;

import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;

/**
 * A file line writer reads string records its input gate and writes them to the associated output file.
 * 
 */
public class FileLineWriter extends AbstractFileOutputTask {

	/**
	 * The record reader through which incoming string records are received.
	 */
	private RecordReader<StringRecord> input = null;


	@Override
	public void invoke() throws Exception {

		Path outputPath = getFileOutputPath();

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


	@Override
	public int getMaximumNumberOfSubtasks() {
		// The default implementation always returns -1
		return -1;
	}
}
