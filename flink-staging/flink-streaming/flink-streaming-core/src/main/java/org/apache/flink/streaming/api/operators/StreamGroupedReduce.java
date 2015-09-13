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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StreamGroupedReduce<IN> extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
		implements OneInputStreamOperator<IN, IN>{

	private static final long serialVersionUID = 1L;

	private KeySelector<IN, ?> keySelector;
	private transient OperatorState<HashMap<Object, IN>> values;

	public StreamGroupedReduce(ReduceFunction<IN> reducer, KeySelector<IN, ?> keySelector) {
		super(reducer);
		this.keySelector = keySelector;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		values = runtimeContext.getOperatorState("flink_internal_reduce_values",
				new HashMap<Object, IN>(), false);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		Object key = keySelector.getKey(element.getValue());

		IN currentValue = values.value().get(key);
		if (currentValue != null) {
			// TODO: find a way to let operators copy elements (maybe)
			IN reduced = userFunction.reduce(currentValue, element.getValue());
			values.value().put(key, reduced);
			output.collect(element.replace(reduced));
		} else {
			values.value().put(key, element.getValue());
			output.collect(element.replace(element.getValue()));
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

}
