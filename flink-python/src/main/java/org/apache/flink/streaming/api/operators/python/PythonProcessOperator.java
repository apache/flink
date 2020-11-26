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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

/**
 * {@link PythonProcessOperator} is responsible for launching beam runner which will start
 * a python harness to execute user defined python ProcessFunction.
 */
@Internal
public class PythonProcessOperator<IN, OUT> extends OneInputPythonFunctionOperator<IN, OUT, Row, OUT>{

	private static final long serialVersionUID = 1L;

	private static final String PROCESS_FUNCTION_URN = "flink:transform:process_function:v1";

	private static final String FLAT_MAP_CODER_URN = "flink:coder:flat_map:v1";

	/**
	 * Reusable row for normal data runner inputs.
	 */
	private transient Row reusableInput;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private transient long currentWatermark;

	public PythonProcessOperator(
		Configuration config,
		TypeInformation<IN> inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config, Types.ROW(Types.LONG, Types.LONG, inputTypeInfo), outputTypeInfo, pythonFunctionInfo);
	}

	@Override
	public void open() throws Exception {
		super.open();
		reusableInput = new Row(3);
		currentWatermark = Long.MIN_VALUE;
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
			bufferedTimestamp.poll();
		} else {
			bais.setBuffer(rawResult, 0, length);
			OUT runnerOutput = runnerOutputTypeSerializer.deserialize(baisWrapper);
			collector.setAbsoluteTimestamp(bufferedTimestamp.peek());
			collector.collect(runnerOutput);
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		reusableInput.setField(0, element.getTimestamp());
		reusableInput.setField(1, currentWatermark);
		reusableInput.setField(2, element.getValue());
		element.replace(reusableInput);
		super.processElement(element);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}

	@Override
	public String getFunctionUrn() {
		return PROCESS_FUNCTION_URN;
	}

	@Override
	public String getCoderUrn() {
		return FLAT_MAP_CODER_URN;
	}
}
