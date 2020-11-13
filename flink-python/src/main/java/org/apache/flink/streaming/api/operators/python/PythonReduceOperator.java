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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

/**
 * {@link PythonReduceOperator} is responsible for launching beam runner which
 * will start a python harness to execute user defined python ReduceFunction.
 */
@Internal
public class PythonReduceOperator<OUT> extends OneInputPythonFunctionOperator<Row, OUT, Row, OUT> {

	private static final long serialVersionUID = 1L;

	private static final String MAP_CODER_URN = "flink:coder:map:v1";

	private static final String STATE_NAME = "_python_reduce_state";

	/**
	 * This state is used to store the currently reduce value.
	 */
	private transient ValueState<OUT> valueState;

	private transient Row reuseRow;

	public PythonReduceOperator(
		Configuration config,
		TypeInformation<Row> inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config, new RowTypeInfo(outputTypeInfo, outputTypeInfo), outputTypeInfo, pythonFunctionInfo);
	}

	@Override
	public void open() throws Exception {
		super.open();

		// create state
		ValueStateDescriptor<OUT> stateId = new ValueStateDescriptor<>(
			STATE_NAME,
			runnerOutputTypeInfo);
		valueState = getPartitionedState(stateId);

		reuseRow = new Row(2);
	}

	@Override
	public void processElement(StreamRecord<Row> element) throws Exception {
		OUT inputData = (OUT) element.getValue().getField(1);
		OUT currentValue = valueState.value();
		if (currentValue == null) {
			// emit directly for the first element.
			valueState.update(inputData);
			collector.setAbsoluteTimestamp(element.getTimestamp());
			collector.collect(inputData);
		} else {
			reuseRow.setField(0, currentValue);
			reuseRow.setField(1, inputData);
			element.replace(reuseRow);
			super.processElement(element);
		}
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawResult, 0, length);
		OUT result = runnerOutputTypeSerializer.deserialize(baisWrapper);
		valueState.update(result);
		collector.setAbsoluteTimestamp(bufferedTimestamp.poll());
		collector.collect(result);
	}

	@Override
	public String getCoderUrn() {
		return MAP_CODER_URN;
	}
}
