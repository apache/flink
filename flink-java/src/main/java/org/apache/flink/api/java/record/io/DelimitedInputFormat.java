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


package org.apache.flink.api.java.record.io;

import org.apache.flink.types.Record;

/**
 * Base implementation for input formats that split the input at a delimiter into records.
 * The parsing of the record bytes into the record has to be implemented in the
 * {@link #readRecord(Record, byte[], int, int)} method.
 * <p>
 * The default delimiter is the newline character {@code '\n'}.
 */
public abstract class DelimitedInputFormat extends org.apache.flink.api.common.io.DelimitedInputFormat<Record> {
	
	private static final long serialVersionUID = -2297199268758915692L;

	// --------------------------------------------------------------------------------------------
	//  User-defined behavior
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This function parses the given byte array which represents a serialized key/value
	 * pair. The parsed content is then returned by setting the pair variables. If the
	 * byte array contains invalid content the record can be skipped by returning <tt>false</tt>.
	 * 
	 * @param reuse The optional reusable holder for the line that is read.
	 * @param bytes The serialized record.
	 * @return returns whether the record was successfully deserialized
	 */
	public abstract Record readRecord(Record reuse, byte[] bytes, int offset, int numBytes);
}
