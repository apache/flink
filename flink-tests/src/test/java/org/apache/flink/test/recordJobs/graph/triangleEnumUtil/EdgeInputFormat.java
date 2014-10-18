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


package org.apache.flink.test.recordJobs.graph.triangleEnumUtil;

import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;


/**
 * 
 */
public final class EdgeInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	public static final String ID_DELIMITER_CHAR = "edgeinput.delimiter";
	
	private final IntValue i1 = new IntValue();
	private final IntValue i2 = new IntValue();
	
	private char delimiter;
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
		final int limit = offset + numBytes;
		int first = 0, second = 0;
		final char delimiter = this.delimiter;
		
		int pos = offset;
		while (pos < limit && bytes[pos] != delimiter) {
			first = first * 10 + (bytes[pos++] - '0');
		}
		pos += 1;// skip the delimiter
		while (pos < limit) {
			second = second * 10 + (bytes[pos++] - '0');
		}
		
		if (first <= 0 || second <= 0 || first == second) {
			return null;
		}
		
		this.i1.setValue(first);
		this.i2.setValue(second);
		target.setField(0, this.i1);
		target.setField(1, this.i2);
		return target;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		this.delimiter = (char) parameters.getInteger(ID_DELIMITER_CHAR, ',');
	}
}
