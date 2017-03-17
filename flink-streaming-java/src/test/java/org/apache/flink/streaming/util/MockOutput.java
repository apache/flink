/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import java.io.Serializable;
import java.util.Collection;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

public class MockOutput<T> implements Output<StreamRecord<T>> {
	private Collection<T> outputs;

	public MockOutput(Collection<T> outputs) {
		this.outputs = outputs;
	}

	@Override
	public void collect(StreamRecord<T> record) {
		T copied = SerializationUtils.deserialize(SerializationUtils.serialize((Serializable) record.getValue()));
		outputs.add(copied);
	}

	@Override
	public <X> void collect(OutputTag<?> outputTag, StreamRecord<X> record) {
		throw new UnsupportedOperationException("Side output not supported for MockOutput");
	}

	@Override
	public void emitWatermark(Watermark mark) {
		throw new RuntimeException("THIS MUST BE IMPLEMENTED");
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		throw new RuntimeException();
	}

	@Override
	public void close() {
	}
}
