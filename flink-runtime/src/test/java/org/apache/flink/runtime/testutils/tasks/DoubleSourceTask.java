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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.io.StringRecord;
import org.apache.flink.runtime.fs.LineReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;

public class DoubleSourceTask extends AbstractInvokable {

	private RecordWriter<StringRecord> output1 = null;

	private RecordWriter<StringRecord> output2 = null;

	@Override
	public void invoke() throws Exception {
		this.output1.initializeSerializers();
		this.output2.initializeSerializers();

		final Iterator<FileInputSplit> splitIterator = getInputSplits();

		while (splitIterator.hasNext()) {

			final FileInputSplit split = splitIterator.next();

			final long start = split.getStart();
			final long length = split.getLength();

			final FileSystem fs = FileSystem.get(split.getPath().toUri());

			final FSDataInputStream fdis = fs.open(split.getPath());

			final LineReader lineReader = new LineReader(fdis, start, length, (1024 * 1024));

			byte[] line = lineReader.readLine();

			while (line != null) {

				// Create a string object from the data read
				StringRecord str = new StringRecord();
				str.set(line);

				// Send out string
				output1.emit(str);
				output2.emit(str);

				line = lineReader.readLine();
			}

			// Close the stream;
			lineReader.close();
		}

		this.output1.flush();
		this.output2.flush();
	}

	@Override
	public void registerInputOutput() {
		this.output1 = new RecordWriter<StringRecord>(this);
		this.output2 = new RecordWriter<StringRecord>(this);
	}

	private Iterator<FileInputSplit> getInputSplits() {

		final InputSplitProvider provider = getEnvironment().getInputSplitProvider();

		return new Iterator<FileInputSplit>() {

			private FileInputSplit nextSplit;
			
			private boolean exhausted;

			@Override
			public boolean hasNext() {
				if (exhausted) {
					return false;
				}
				
				if (nextSplit != null) {
					return true;
				}
				
				FileInputSplit split = (FileInputSplit) provider.getNextInputSplit();
				
				if (split != null) {
					this.nextSplit = split;
					return true;
				}
				else {
					exhausted = true;
					return false;
				}
			}

			@Override
			public FileInputSplit next() {
				if (this.nextSplit == null && !hasNext()) {
					throw new NoSuchElementException();
				}

				final FileInputSplit tmp = this.nextSplit;
				this.nextSplit = null;
				return tmp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}