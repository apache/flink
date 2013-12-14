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

import java.util.Iterator;

import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.runtime.fs.LineReader;

/**
 * A file line reader reads the associated file input splits line by line and outputs the lines as string records.
 * 
 * @author warneke
 */
public class FileLineReader extends AbstractFileInputTask {

	private RecordWriter<StringRecord> output = null;

	@Override
	public void invoke() throws Exception {

		final Iterator<FileInputSplit> splitIterator = getFileInputSplits();

		while (splitIterator.hasNext()) {

			final FileInputSplit split = splitIterator.next();

			long start = split.getStart();
			long length = split.getLength();

			final FileSystem fs = FileSystem.get(split.getPath().toUri());

			final FSDataInputStream fdis = fs.open(split.getPath());

			final LineReader lineReader = new LineReader(fdis, start, length, (1024 * 1024));

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
