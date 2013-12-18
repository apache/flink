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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import eu.stratosphere.nephele.types.FileRecord;

public class DirectoryReader extends AbstractFileInputTask {

	/**
	 * The record writer to write the output strings to.
	 */
	private RecordWriter<FileRecord> output = null;

	// buffer
	private byte[] buffer;

	private static final Log LOG = LogFactory.getLog(DirectoryReader.class);


	@Override
	public void invoke() throws Exception {

		final Iterator<FileInputSplit> splitIterator = getFileInputSplits();
		FileRecord fr = null;

		while (splitIterator.hasNext()) {

			final FileInputSplit split = splitIterator.next();

			final long start = split.getStart();
			final long end = start + split.getLength();

			if (buffer == null || buffer.length < end - start) {
				buffer = new byte[(int) (end - start)];
			}

			if (fr == null || fr.getFileName().compareTo(split.getPath().getName()) != 0) {
				if (fr != null) {
					try {
						output.emit(fr);
					} catch (InterruptedException e) {
						// TODO: Respond to interruption properly
						LOG.error(e);
					}
				}
				fr = new FileRecord(split.getPath().getName());
			}

			final FileSystem fs = FileSystem.get(split.getPath().toUri());

			final FSDataInputStream fdis = fs.open(split.getPath());
			fdis.seek(split.getStart());

			int read = fdis.read(buffer, 0, buffer.length);
			if (read == -1)
				continue;

			fr.append(buffer, 0, read);

			if (read != end - start) {
				System.err.println("Unexpected number of bytes read! Expected: " + (end - start) + " Read: " + read);
			}
		}

		if (fr != null)
			try {
				output.emit(fr);
			} catch (InterruptedException e) {
				// TODO: Respond to interruption properly
				LOG.error(e);
			}
	}


	@Override
	public void registerInputOutput() {
		output = new RecordWriter<FileRecord>(this, FileRecord.class);
	}

}
