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

package org.apache.flink.table.functions.python;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The list of the built-in aggregate functions which can be mixed with the Python UDAF.
 */
public enum BuiltInPythonAggregateFunction implements PythonFunction {

	/*
	The list of the Python built-in aggregate functions.
	 */
	AVG("AvgAggFunction"),
	COUNT1("Count1AggFunction"),
	COUNT("CountAggFunction"),
	FIRST_VALUE("FirstValueAggFunction"),
	FIRST_VALUE_RETRACT("FirstValueWithRetractAggFunction"),
	LAST_VALUE("LastValueAggFunction"),
	LAST_VALUE_RETRACT("LastValueWithRetractAggFunction"),
	LIST_AGG("ListAggFunction"),
	LIST_AGG_RETRACT("ListAggWithRetractAggFunction"),
	LIST_AGG_WS_RETRACT("ListAggWsWithRetractAggFunction"),
	MAX("MaxAggFunction"),
	MAX_RETRACT("MaxWithRetractAggFunction"),
	MIN("MinAggFunction"),
	MIN_RETRACT("MinWithRetractAggFunction"),
	INT_SUM0("IntSum0AggFunction"),
	FLOAT_SUM0("FloatSum0AggFunction"),
	DECIMAL_SUM0("DecimalSum0AggFunction"),
	SUM("SumAggFunction"),
	SUM_RETRACT("SumWithRetractAggFunction");

	private final byte[] payload;

	BuiltInPythonAggregateFunction(String pythonFunctionName) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// the first byte '0' represents current payload contains a built-in function.
		baos.write(0);
		try {
			baos.write(pythonFunctionName.getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			throw new RuntimeException(
				"Exception thrown when creating the python built-in aggregate function enum.", e);
		}
		payload = baos.toByteArray();
	}

	@Override
	public byte[] getSerializedPythonFunction() {
		return payload;
	}

	@Override
	public PythonEnv getPythonEnv() {
		return new PythonEnv(PythonEnv.ExecType.PROCESS);
	}
}
