/*
 * /* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.storm.api;

import org.apache.flink.storm.wrappers.StormTuple;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;

/**
 * Merges two Flink {@link DataStreams} into a stream of type {@link StormTuple}.
 */
@SuppressWarnings("rawtypes")
final class TwoFlinkStreamsMerger<IN1, IN2> implements CoFlatMapFunction<IN1, IN2, StormTuple> {
	private static final long serialVersionUID = -495174165824062256L;

	private final String inputStreamId1;
	private final String inputComponentId1;
	private final Fields inputSchema1;
	private final String inputStreamId2;
	private final String inputComponentId2;
	private final Fields inputSchema2;

	public TwoFlinkStreamsMerger(GlobalStreamId streamId1, Fields schema1, GlobalStreamId streamId2,
			Fields schema2) {
		this.inputStreamId1 = streamId1.get_streamId();
		this.inputComponentId1 = streamId1.get_componentId();
		this.inputSchema1 = schema1;
		this.inputStreamId2 = streamId2.get_streamId();
		this.inputComponentId2 = streamId2.get_componentId();
		this.inputSchema2 = schema2;
	}

	@Override
	public void flatMap1(IN1 value, Collector<StormTuple> out) throws Exception {
		out.collect(new StormTuple<IN1>(value, this.inputSchema1, 0,
				this.inputStreamId1, this.inputComponentId1, MessageId.makeUnanchored()));
	}

	@Override
	public void flatMap2(IN2 value, Collector<StormTuple> out) throws Exception {
		out.collect(new StormTuple<IN2>(value, this.inputSchema2, 0,
				this.inputStreamId2, this.inputComponentId2, MessageId.makeUnanchored()));
	}
}
