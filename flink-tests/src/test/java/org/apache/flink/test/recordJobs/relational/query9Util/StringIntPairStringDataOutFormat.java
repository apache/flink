/*
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



package org.apache.flink.test.recordJobs.relational.query9Util;

import java.io.IOException;

import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;

public class StringIntPairStringDataOutFormat extends FileOutputFormat {
	private static final long serialVersionUID = 1L;

	private final StringBuilder buffer = new StringBuilder();
	private StringIntPair key = new StringIntPair();
	private StringValue value = new StringValue();
	
	@Override
	public void writeRecord(Record record) throws IOException {
		key = record.getField(0, key);
		value = record.getField(1, value);
		
		this.buffer.setLength(0);
		this.buffer.append(key.getFirst().toString());
		this.buffer.append('|');
		this.buffer.append(key.getSecond().toString());
		this.buffer.append('|');
		this.buffer.append(value.toString());
		this.buffer.append('\n');
		
		byte[] bytes = this.buffer.toString().getBytes();
		
		this.stream.write(bytes);
	}

}
