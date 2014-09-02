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


package org.apache.flink.test.recordJobs.sort.tsUtil;

import java.io.IOException;

import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.types.Record;


/**
 * The class is responsible for converting a two field record back into a line which is afterward written back to disk.
 * Each line ends with a newline character.
 * 
 */
public final class TeraOutputFormat extends FileOutputFormat {
	private static final long serialVersionUID = 1L;

	/**
	 * A buffer to store the line which is about to be written back to disk.
	 */
	private final byte[] buffer = new byte[TeraKey.KEY_SIZE + TeraValue.VALUE_SIZE + 1];


	@Override
	public void writeRecord(Record record) throws IOException {
		record.getField(0, TeraKey.class).copyToBuffer(this.buffer);
		record.getField(1, TeraValue.class).copyToBuffer(this.buffer);

		this.buffer[TeraKey.KEY_SIZE + TeraValue.VALUE_SIZE] = '\n';

		this.stream.write(buffer, 0, buffer.length);
	}

}
