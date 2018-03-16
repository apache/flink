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
 * This class is a {@link PythonSender} for operations with two input streams.
 *
 * @param <IN1> first input type
 * @param <IN2> second input type
 */
public class PythonDualInputSender<IN1, IN2> extends PythonSender {

	private static final long serialVersionUID = 614115041181108878L;

	private transient Serializer<IN1> serializer1;
	private transient Serializer<IN2> serializer2;

	protected PythonDualInputSender(Configuration config) {
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
	public int sendBuffer1(SingleElementPushBackIterator<IN1> input) throws IOException {
		if (serializer1 == null) {
			IN1 value = input.next();
			serializer1 = getSerializer(value);
			input.pushBack(value);
		}
		return sendBuffer(input, serializer1);
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
	public int sendBuffer2(SingleElementPushBackIterator<IN2> input) throws IOException {
		if (serializer2 == null) {
			IN2 value = input.next();
			serializer2 = getSerializer(value);
			input.pushBack(value);
		}
		return sendBuffer(input, serializer2);
	}
}
