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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;

/**
 * The {@link PythonFlatMapOperator} is responsible for executing Python functions that gets one
 * input and produces zero/one or more outputs.
 *
 * @param <IN>     The type of the input elements
 * @param <OUT>The type of the output elements
 */
public class PythonFlatMapOperator<IN, OUT> extends OneInputPythonFunctionOperator<IN, OUT> {
	private static final long serialVersionUID = 1L;

	public PythonFlatMapOperator(
		Configuration config,
		TypeInformation<IN> inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config, inputTypeInfo, outputTypeInfo, pythonFunctionInfo);
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		if (PythonOperatorUtils.endOfLastFlatMap(resultTuple.f1, resultTuple.f0)) {
			bufferedTimestamp.poll();
		} else {
			byte[] rawResult = resultTuple.f0;
			int length = resultTuple.f1;
			bais.setBuffer(rawResult, 0, length);
			collector.setAbsoluteTimestamp(bufferedTimestamp.peek());
			collector.collect(outputTypeSerializer.deserialize(baisWrapper));
		}
	}
}
