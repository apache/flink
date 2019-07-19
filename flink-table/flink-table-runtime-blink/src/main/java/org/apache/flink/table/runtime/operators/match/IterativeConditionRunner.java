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

package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

/**
 * A {@link RichIterativeCondition} wrapper to delegate invocation to the code generated
 * {@link RichIterativeCondition}.
 */
public class IterativeConditionRunner extends RichIterativeCondition<BaseRow> {
	private static final long serialVersionUID = 1L;

	private final GeneratedFunction<RichIterativeCondition<BaseRow>> generatedFunction;
	private transient RichIterativeCondition<BaseRow> function;

	public IterativeConditionRunner(GeneratedFunction<RichIterativeCondition<BaseRow>> generatedFunction) {
		this.generatedFunction = generatedFunction;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.function = generatedFunction.newInstance(getRuntimeContext().getUserCodeClassLoader());
		FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext());
		FunctionUtils.openFunction(function, parameters);
	}

	@Override
	public boolean filter(BaseRow value, Context<BaseRow> ctx) throws Exception {
		return function.filter(value, ctx);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(function);
	}
}
