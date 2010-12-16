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

package eu.stratosphere.nephele.io.library;

import java.io.IOException;

import eu.stratosphere.nephele.execution.ExecutionFailureException;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A file line writer reads string records its input gate and writes them to the associated output file.
 * 
 * @author warneke
 */
public class FileLineWriter extends AbstractFileOutputTask {

	private RecordReader<StringRecord> input = null;

	@Override
	public void invoke() throws ExecutionFailureException {

		try {

			Path outputPath = getFileOutputPath();

			FileSystem fs = FileSystem.get(outputPath.toUri());
			if (fs.exists(outputPath)) {
				FileStatus status = fs.getFileStatus(outputPath);

				if (status.isDir()) {
					outputPath = new Path(outputPath.toUri().toString() + "/file_" + getIndexInSubtaskGroup() + ".txt");
				}
			}

			FSDataOutputStream outputStream = fs.create(outputPath, true);

			int i = 0;

			while (input.hasNext()) {

				try {
					StringRecord record = input.next();
					i++;
					byte[] recordByte = (record.toString() + "\r\n").getBytes();
					outputStream.write(recordByte, 0, recordByte.length);
					// TODO: Implement me
					// System.out.println(input.next());
					// TODO Auto-generated catch block
				} catch (InterruptedException e) {
					// TODO: Handle interruption properly
					e.printStackTrace();
				}

			}

			System.out.println("WRITER: Wrote " + i + "records ");

			outputStream.close();

		} catch (IOException ioe) {
			throw new ExecutionFailureException(StringUtils.stringifyException(ioe));
		}
	}

	@Override
	public void registerInputOutput() {
		input = new RecordReader<StringRecord>(this, StringRecord.class, new PointwiseDistributionPattern());
	}

	@Override
	public int getMaximumNumberOfSubtasks() {
		// The default implementation always returns -1
		return -1;
	}
}
