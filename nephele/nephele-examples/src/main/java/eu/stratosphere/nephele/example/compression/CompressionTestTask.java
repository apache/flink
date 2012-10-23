/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.example.compression;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.FileRecord;

public class CompressionTestTask extends AbstractTask {

	private RecordReader<FileRecord> input = null;

	private RecordWriter<FileRecord> output = null;

	@Override
	public void invoke() throws Exception {

		while (this.input.hasNext()) {

			// Simply forward the records
			FileRecord f;
			try {
				f = this.input.next();
				System.err.println("now processing file: " + f.getFileName());
				this.output.emit(f);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<FileRecord>(this, FileRecord.class);
		this.output = new RecordWriter<FileRecord>(this);
	}

}
