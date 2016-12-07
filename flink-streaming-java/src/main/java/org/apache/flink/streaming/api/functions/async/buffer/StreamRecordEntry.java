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

package org.apache.flink.streaming.api.functions.async.buffer;

import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * {@link AsyncCollectorBuffer} entry for {@link StreamRecord}
 *
 * @param <IN> Input data type
 * @param <OUT> Output data type
 */
public class StreamRecordEntry<IN, OUT> extends AbstractBufferEntry<OUT> implements AsyncCollector<OUT> {
	private List<OUT> result;
	private Throwable error;

	private boolean isDone = false;

	private final AsyncCollectorBuffer<IN, OUT> buffer;

	public StreamRecordEntry(StreamRecord<IN> element, AsyncCollectorBuffer<IN, OUT> buffer) {
		super(element);
		this.buffer = Preconditions.checkNotNull(buffer, "Reference to AsyncCollectorBuffer should not be null");
	}

	@Override
	public void collect(List<OUT> result)  {
		this.result = result;

		this.buffer.markCollectorCompleted(this);
	}

	@Override
	public void collect(Throwable error)  {
		this.error = error;

		this.buffer.markCollectorCompleted(this);
	}

	public List<OUT> getResult() throws IOException {
		if (error != null) {
			throw new IOException(error.getMessage());
		}
		return result;
	}

	public void markDone() {
		isDone = true;
	}

	public boolean isDone() {
		return isDone;
	}
}
