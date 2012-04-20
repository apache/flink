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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.types.FileRecord;

public class DirectoryWriter extends AbstractFileOutputTask {

	/**
	 * The record reader to read the incoming strings from.
	 */
	private RecordReader<FileRecord> input = null;

	private static final Log LOG = LogFactory.getLog(DirectoryWriter.class);

	@Override
	public void invoke() throws Exception {

		final Path path = getFileOutputPath();
		final FileSystem fs = path.getFileSystem();

		try {
			while (input.hasNext()) {

				final FileRecord record = input.next();
				Path newPath = new Path(path + Path.SEPARATOR + record.getFileName());
				FSDataOutputStream outputStream = fs.create(newPath, true);

				outputStream.write(record.getDataBuffer(), 0, record.getDataBuffer().length);
				outputStream.close();
				// TODO: Implement me
				// System.out.println(input.next());
				// ODO Auto-generated catch block
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error(e);
		}

	}

	@Override
	public void registerInputOutput() {
		input = new RecordReader<FileRecord>(this, FileRecord.class, null);

	}

}
