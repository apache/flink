/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.watermark;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * A Watermark tells operators that receive it that no elements with a timestamp older or equal
 * to the watermark timestamp should arrive at the operator. Watermarks are emitted at the
 * sources and propagate through the operators of the topology. Operators must themselves emit
 * watermarks to downstream operators using
 * {@link org.apache.flink.streaming.api.operators.Output#emitWatermark(Watermark)}. Operators that
 * do not internally buffer elements can always forward the watermark that they receive. Operators
 * that buffer elements, such as window operators, must forward a watermark after emission of
 * elements that is triggered by the arriving watermark.
 *
 * <p>
 * In some cases a watermark is only a heuristic and operators should be able to deal with
 * late elements. They can either discard those or update the result and emit updates/retractions
 * to downstream operations.
 *
 */
public class Watermark extends TaskEvent{

	private Instant timestamp;

	/**
	 * This specifies the input index on which the watermark arrived. For one-input operators
	 * this will always be 0, for two-input operators this can be 0 or 1.
	 */
	private int inputIndex;

	/**
	 * Creates a new watermark without a timestamp. This is only necessary because
	 * {@link Watermark} is an {@link org.apache.flink.core.io.IOReadableWritable}.
	 */
	public Watermark() {
		this.timestamp = null;
		this.inputIndex = 0;
	}

	/**
	 * Creates a new watermark with the given timestamp.
	 */
	public Watermark(Instant timestamp) {
		this.timestamp = timestamp;
		this.inputIndex = 0;
	}

	/**
	 * Creates a new watermark with the given timestamp and input index.
	 */
	public Watermark(Instant timestamp, int inputIndex) {
		this.timestamp = timestamp;
		this.inputIndex = inputIndex;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(timestamp.getMillis());
		out.writeInt(inputIndex);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		timestamp = new Instant(in.readLong());
		inputIndex = in.readInt();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Watermark watermark = (Watermark) o;

		return inputIndex == watermark.inputIndex && timestamp.equals(watermark.timestamp);
	}

	@Override
	public int hashCode() {
		int result = timestamp.hashCode();
		result = 31 * result + inputIndex;
		return result;
	}

	@Override
	public String toString() {
		return "Watermark{" +
				"timestamp=" + timestamp +
				", inputIndex=" + inputIndex +
				'}';
	}
}
