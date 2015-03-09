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

package org.apache.flink.streaming.io;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.reader.StreamingAbstractRecordReader;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

public class StreamingMutableRecordReader<T extends IOReadableWritable> extends
		StreamingAbstractRecordReader<T> implements MutableReader<T> {

	public StreamingMutableRecordReader(InputGate inputGate) {
		super(inputGate);
	}

	@Override
	public boolean next(final T target) throws IOException, InterruptedException {
		return getNextRecord(target);
	}

	@Override
	public void clearBuffers() {
		super.clearBuffers();
	}
}
