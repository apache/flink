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


package org.apache.flink.runtime.jobmanager;

import org.apache.flink.core.io.StringRecord;
import org.apache.flink.runtime.io.network.api.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.io.network.api.UnionRecordReader;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

/**
 * A simple implementation of a task using a {@link UnionRecordReader}.
 */
public class UnionTask extends AbstractInvokable {

	/**
	 * The union record reader to be used during the tests.
	 */
	private UnionRecordReader<StringRecord> unionReader;

	private RecordWriter<StringRecord> writer;
	
	
	@Override
	public void registerInputOutput() {

		@SuppressWarnings("unchecked")
		MutableRecordReader<StringRecord>[] recordReaders = (MutableRecordReader<StringRecord>[]) new MutableRecordReader<?>[2];
		recordReaders[0] = new MutableRecordReader<StringRecord>(this);
		recordReaders[1] = new MutableRecordReader<StringRecord>(this);
		this.unionReader = new UnionRecordReader<StringRecord>(recordReaders, StringRecord.class);
		
		this.writer = new RecordWriter<StringRecord>(this);
	}

	@Override
	public void invoke() throws Exception {
		this.writer.initializeSerializers();

		while (this.unionReader.hasNext()) {
			this.writer.emit(this.unionReader.next());
		}

		this.writer.flush();
	}
}