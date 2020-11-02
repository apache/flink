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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing a {@link ReduceFunction} on a
 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} in a
 * {@link RuntimeExecutionMode#BATCH} mode.
 */
@Internal
public class BatchGroupedReduceOperator<IN, KEY>
		extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
		implements OneInputStreamOperator<IN, IN>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private static final String STATE_NAME = "_op_state";

	private transient ValueState<IN> values;

	private final TypeSerializer<IN> serializer;

	private InternalTimerService<VoidNamespace> timerService;

	public BatchGroupedReduceOperator(ReduceFunction<IN> reducer, TypeSerializer<IN> serializer) {
		super(reducer);
		this.serializer = serializer;
	}

	@Override
	public void open() throws Exception {
		super.open();
		ValueStateDescriptor<IN> stateId = new ValueStateDescriptor<>(STATE_NAME, serializer);
		values = getPartitionedState(stateId);
		timerService = getInternalTimerService(
			"end-key-timers",
			new VoidNamespaceSerializer(),
			this
		);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		IN currentValue = values.value();

		if (currentValue != null) {
			value = userFunction.reduce(currentValue, value);
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, Long.MAX_VALUE);
		}
		values.update(value);
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		IN currentValue = values.value();
		if (currentValue != null) {
			output.collect(new StreamRecord<>(currentValue, Long.MAX_VALUE));
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
	}
}
