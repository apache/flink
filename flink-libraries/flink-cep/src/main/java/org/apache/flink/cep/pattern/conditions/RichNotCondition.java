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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;

/**
 * A {@link RichIterativeCondition condition} which negates the condition it wraps
 * and returns {@code true} if the original condition returns {@code false}.
 *
 * @param <T> Type of the element to filter
 */
public class RichNotCondition<T> extends RichIterativeCondition<T> {

	private static final long serialVersionUID = 1L;

	private final IterativeCondition<T> original;

	public RichNotCondition(final IterativeCondition<T> original) {
		this.original = original;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);
		if (original != null) {
			FunctionUtils.setFunctionRuntimeContext(original, t);
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (original != null) {
			FunctionUtils.openFunction(original, parameters);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (original != null) {
			FunctionUtils.closeFunction(original);
		}
	}

	@Override
	public boolean filter(T value, Context<T> ctx) throws Exception {
		return original != null && !original.filter(value, ctx);
	}
}
