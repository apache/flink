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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;

/**
 * Wrapper around {@link Function}.
 * @param <T>
 */
@Internal
public abstract class WrappingFunction<T extends Function> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	protected T wrappedFunction;

	protected WrappingFunction(T wrappedFunction) {
		this.wrappedFunction = wrappedFunction;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		FunctionUtils.openFunction(this.wrappedFunction, parameters);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(this.wrappedFunction);
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);

		FunctionUtils.setFunctionRuntimeContext(this.wrappedFunction, t);
	}

	public T getWrappedFunction () {
		return this.wrappedFunction;
	}
}
