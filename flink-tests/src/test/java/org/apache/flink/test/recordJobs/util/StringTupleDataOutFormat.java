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


package org.apache.flink.test.recordJobs.util;

import org.apache.flink.api.java.record.io.DelimitedOutputFormat;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;

public class StringTupleDataOutFormat extends DelimitedOutputFormat {
	private static final long serialVersionUID = 1L;

	@Override
	public int serializeRecord(Record rec, byte[] target) throws Exception {
		String string = rec.getField(0, StringValue.class).toString();
		byte[] stringBytes = string.getBytes();
		Tuple tuple = rec.getField(1, Tuple.class);
		String tupleStr = tuple.toString();
		byte[] tupleBytes = tupleStr.getBytes();
		int totalLength = stringBytes.length + 1 + tupleBytes.length;
		if(target.length >= totalLength) {
			System.arraycopy(stringBytes, 0, target, 0, stringBytes.length);
			target[stringBytes.length] = '|';
			System.arraycopy(tupleBytes, 0, target, stringBytes.length + 1, tupleBytes.length);
			return totalLength;
		} else {
			return -1 * totalLength;
		}
	}

}
