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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.PersistOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

/**
 * This operator represents the application of a "persist" function on a data set. It persists
 * the input of the operator in memory and directly serves from there when accessed again.
 *
 * @param <T> The type of the data set consumed by the operator.
 */
public class PersistOperator<T> extends SingleInputUdfOperator<T, T, PersistOperator<T>> {

	protected final NoOpFunction function;

	protected final String defaultName;

	public PersistOperator(DataSet<T> input, TypeInformation<T> resultType, String defaultName) {
		super(input, resultType);

		this.defaultName = defaultName;
		this.function = new NoOpFunction();

		UdfOperatorUtils.analyzeSingleInputUdf(this, NoOpFunction.class, defaultName, function, null);
	}

	@Override
	protected NoOpFunction getFunction() {
		return function;
	}

	@Override
	protected PersistOperatorBase<T> translateToDataFlow(Operator<T> input) {

		String name = getName() != null ? getName() : "Persist at " + defaultName;
		// create operator
		PersistOperatorBase<T> po = new PersistOperatorBase<>(function, new UnaryOperatorInformation<T, T>(getInputType(), getResultType()), name);
		// set input
		po.setInput(input);
		// set parallelism
		po.setParallelism(input.getParallelism());

		return po;
	}

	@Override
	public PersistOperator<T> setParallelism(int parallelism) {
		throw new UnsupportedOperationException("Parallelism of Persist operator is exactly the same as its input");
	}

	// ------------------- disallow some functions -----------------------------------------------
	@Override
	public PersistOperator<T> withBroadcastSet(DataSet<?> data, String name) {
		throw new UnsupportedOperationException("Persist operator doesn't allow broadcast inputs");
	}
}
