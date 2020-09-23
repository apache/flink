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

package org.apache.flink.table.runtime.operators.python.utils;

import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.functions.python.PythonFunctionInfo;

import com.google.protobuf.ByteString;

/**
 * The collectors used to collect Row values.
 */
public enum PythonOperatorUtils {
	;

	public static FlinkFnApi.UserDefinedFunction getUserDefinedFunctionProto(PythonFunctionInfo pythonFunctionInfo) {
		FlinkFnApi.UserDefinedFunction.Builder builder = FlinkFnApi.UserDefinedFunction.newBuilder();
		builder.setPayload(ByteString.copyFrom(pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		for (Object input : pythonFunctionInfo.getInputs()) {
			FlinkFnApi.UserDefinedFunction.Input.Builder inputProto =
				FlinkFnApi.UserDefinedFunction.Input.newBuilder();
			if (input instanceof PythonFunctionInfo) {
				inputProto.setUdf(getUserDefinedFunctionProto((PythonFunctionInfo) input));
			} else if (input instanceof Integer) {
				inputProto.setInputOffset((Integer) input);
			} else {
				inputProto.setInputConstant(ByteString.copyFrom((byte[]) input));
			}
			builder.addInputs(inputProto);
		}
		return builder.build();
	}

}
