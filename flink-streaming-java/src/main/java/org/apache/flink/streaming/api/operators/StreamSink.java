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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing {@link SinkFunction SinkFunctions}.
 */
@Internal
public class StreamSink<IN> extends AbstractUdfStreamOperator<Object, SinkFunction<IN>>
		implements OneInputStreamOperator<IN, Object> {

	private static final long serialVersionUID = 1L;

	public StreamSink(SinkFunction<IN> sinkFunction) {
		super(sinkFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		userFunction.invoke(element.getValue());
	}

	@Override
	protected void reportOrForwardLatencyMarker(LatencyMarker maker) {
		// all operators are tracking latencies
		this.latencyGauge.reportLatency(maker, true);

		// sinks don't forward latency markers
	}
}
