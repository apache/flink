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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Persists the input of the operator in memory.
 *
 * @param <T> The input type.
 */
public class PersistOperatorBase<T> extends SingleInputOperator<T, T, NoOpFunction> {

	public PersistOperatorBase(NoOpFunction udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<NoOpFunction>(udf), operatorInfo, name);
	}

	@Override
	protected List<T> executeOnCollections(List<T> inputData, RuntimeContext ctx, ExecutionConfig executionConfig) throws Exception {
		ArrayList<T> result = new ArrayList<T>(inputData.size());

		TypeSerializer<T> inSerializer = getOperatorInfo().getInputType().createSerializer(executionConfig);
		TypeSerializer<T> outSerializer = getOperatorInfo().getOutputType().createSerializer(executionConfig);

		for (T element : inputData) {
			T out = inSerializer.copy(element);
			result.add(outSerializer.copy(out));
		}

		return result;
	}

	// ----------  Disallow some functions --------------------------------------------------------

	@Override
	public Map<String, Operator<?>> getBroadcastInputs() {
		return this.broadcastInputs;
	}

	@Override
	public void setBroadcastVariable(String name, Operator<?> root) {
		throw new UnsupportedOperationException("Persist operator doesn't allow broadcast inputs");
	}

	@Override
	public <BC> void setBroadcastVariables(Map<String, Operator<BC>> inputs) {
		throw new UnsupportedOperationException("Persist operator doesn't allow broadcast inputs");
	}
}
