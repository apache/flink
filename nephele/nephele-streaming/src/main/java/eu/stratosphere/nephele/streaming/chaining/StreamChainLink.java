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

package eu.stratosphere.nephele.streaming.chaining;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.types.Record;

public final class StreamChainLink {

	private final Mapper<? extends Record, ? extends Record> mapper;

	private final RecordReader<? extends Record> reader;

	private final RecordWriter<? extends Record> writer;

	StreamChainLink(final Mapper<? extends Record, ? extends Record> mapper, RecordReader<? extends Record> reader,
			RecordWriter<? extends Record> writer) {

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;
	}

	Mapper<? extends Record, ? extends Record> getMapper() {

		return this.mapper;
	}

	RecordReader<? extends Record> getReader() {

		return this.reader;
	}

	RecordWriter<? extends Record> getWriter() {

		return this.writer;
	}
}
