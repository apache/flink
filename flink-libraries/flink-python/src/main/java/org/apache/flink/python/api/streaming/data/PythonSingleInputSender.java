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

package org.apache.flink.python.api.streaming.data;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;

/**
 * This class is a {@link PythonSender} for operations with one input stream.
 *
 * @param <IN> input type
 */
public class PythonSingleInputSender<IN> extends PythonSender {

	private static final long serialVersionUID = 614115041181108878L;

	private transient Serializer<IN> serializer;

	protected PythonSingleInputSender(Configuration config) {
		super(config);
	}

	/**
	 * Extracts records from an iterator and writes them to the memory-mapped file. This method assumes that all values
	 * in the iterator are of the same type. This method does NOT take care of synchronization. The caller must
	 * guarantee that the file may be written to before calling this method.
	 *
	 * @param input iterator containing records
	 * @return size of the written buffer
	 * @throws IOException
	 */
	public int sendBuffer(SingleElementPushBackIterator<IN> input) throws IOException {
		if (serializer == null) {
			IN value = input.next();
			serializer = getSerializer(value);
			input.pushBack(value);
		}
		return sendBuffer(input, serializer);
	}
}
