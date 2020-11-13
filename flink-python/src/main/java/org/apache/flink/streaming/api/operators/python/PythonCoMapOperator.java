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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.types.Row;

/**
 * The {@link PythonCoFlatMapOperator} is responsible for executing the Python CoMap Function.
 *
 * @param <IN1> The input type of the first stream
 * @param <IN2> The input type of the second stream
 * @param <OUT> The output type of the CoMap function
 */
@Internal
public class PythonCoMapOperator<IN1, IN2, OUT> extends TwoInputPythonFunctionOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	private static final String MAP_CODER_URN = "flink:coder:map:v1";

	public PythonCoMapOperator(
		Configuration config,
		TypeInformation<IN1> inputTypeInfo1,
		TypeInformation<IN2> inputTypeInfo2,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo,
		boolean isKeyedStream) {
		super(config, inputTypeInfo1, inputTypeInfo2, outputTypeInfo, pythonFunctionInfo, isKeyedStream);
	}

	@Override
	public String getFunctionUrn() {
		return MAP_CODER_URN;
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawResult, 0, length);
		Row outputRow = runnerOutputTypeSerializer.deserialize(baisWrapper);
		if ((byte) outputRow.getField(0) == PythonOperatorUtils.CoMapFunctionOutputFlag.LEFT.value) {
			collector.setAbsoluteTimestamp(bufferedTimestamp1.poll());
		} else {
			collector.setAbsoluteTimestamp(bufferedTimestamp2.poll());
		}
		collector.collect(outputRow.getField(1));
	}
}
