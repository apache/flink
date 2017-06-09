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
 * Merges a stream of type {@link StormTuple} with a Flink {@link DataStreams} into a stream of type {@link StormTuple}.
 */
@SuppressWarnings("rawtypes")
final class StormFlinkStreamMerger<IN1, IN2> implements CoFlatMapFunction<StormTuple<IN1>, IN2, StormTuple> {
	private static final long serialVersionUID = -914164633830563631L;

	private final String inputStreamId;
	private final String inputComponentId;
	private final Fields inputSchema;

	public StormFlinkStreamMerger(GlobalStreamId streamId, Fields schema) {
		this.inputStreamId = streamId.get_streamId();
		this.inputComponentId = streamId.get_componentId();
		this.inputSchema = schema;
	}

	@Override
	public void flatMap1(StormTuple<IN1> value, Collector<StormTuple> out) throws Exception {
		out.collect(value);
	}

	@Override
	public void flatMap2(IN2 value, Collector<StormTuple> out) throws Exception {
		out.collect(new StormTuple<IN2>(value, this.inputSchema, 0, this.inputStreamId,
				this.inputComponentId, MessageId.makeUnanchored()));
	}
}
