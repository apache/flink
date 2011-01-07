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

import eu.stratosphere.nephele.execution.ExecutionFailureException;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * A file line reader reads the associated file input splits line by line and outputs the lines as string records.
 * 
 * @author warneke
 */
public class FileLineReader extends AbstractFileInputTask {

	private RecordWriter<StringRecord> output = null;

	@Override
	public void invoke() throws Exception {

		FileInputSplit[] splits = getFileInputSplits();

		for (int i = 0; i < splits.length; i++) {

			FileInputSplit split = splits[i];

			long start = split.getStart();
			long length = split.getLength();

			FileSystem fs = FileSystem.get(split.getPath().toUri());

			FSDataInputStream fdis = fs.open(split.getPath());

			LineReader lineReader = new LineReader(fdis, start, length, (1024 * 1024));

			byte[] line = lineReader.readLine();

			while (line != null) {

				// Create a string object from the data read
				StringRecord str = new StringRecord();
				str.set(line);

				// Send out string
				output.emit(str);

				line = lineReader.readLine();
			}

			// Close the stream;
			lineReader.close();
		}
	}

	@Override
	public void registerInputOutput() {
		output = new RecordWriter<StringRecord>(this, StringRecord.class);
	}

}
