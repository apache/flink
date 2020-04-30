/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

/**
 * Internal {@link ProcessWindowFunction} that is used for implementing a fold on a window
 * configuration that only allows {@link ProcessWindowFunction} and cannot directly execute a
 * {@link FoldFunction}.
 *
 * @deprecated will be removed in a future version
 */
@Internal
@Deprecated
public class FoldApplyProcessWindowFunction<K, W extends Window, T, ACC, R>
	extends ProcessWindowFunction<T, R, K, W>
	implements OutputTypeConfigurable<R> {

	private static final long serialVersionUID = 1L;

	private final FoldFunction<T, ACC> foldFunction;
	private final ProcessWindowFunction<ACC, R, K, W> windowFunction;

	private byte[] serializedInitialValue;
	private TypeSerializer<ACC> accSerializer;
	private final TypeInformation<ACC> accTypeInformation;
	private transient ACC initialValue;
	private transient InternalProcessApplyWindowContext<ACC, R, K, W> ctx;

	public FoldApplyProcessWindowFunction(ACC initialValue, FoldFunction<T, ACC> foldFunction, ProcessWindowFunction<ACC, R, K, W> windowFunction, TypeInformation<ACC> accTypeInformation) {
		this.windowFunction = windowFunction;
		this.foldFunction = foldFunction;
		this.initialValue = initialValue;
		this.accTypeInformation = accTypeInformation;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		FunctionUtils.openFunction(this.windowFunction, configuration);

		if (serializedInitialValue == null) {
			throw new RuntimeException("No initial value was serialized for the fold " +
				"window function. Probably the setOutputType method was not called.");
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(serializedInitialValue);
		DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);
		initialValue = accSerializer.deserialize(in);

		ctx = new InternalProcessApplyWindowContext<>(windowFunction);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(this.windowFunction);
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);

		FunctionUtils.setFunctionRuntimeContext(this.windowFunction, t);
	}

	@Override
	public void process(K key, Context context, Iterable<T> values, Collector<R> out) throws Exception {
		ACC result = accSerializer.copy(initialValue);

		for (T val : values) {
			result = foldFunction.fold(result, val);
		}

		this.ctx.window = context.window();
		this.ctx.context = context;
		windowFunction.process(key, ctx, Collections.singletonList(result), out);
	}

	@Override
	public void clear(final Context context) throws Exception{
		this.ctx.window = context.window();
		this.ctx.context = context;
		windowFunction.clear(ctx);
	}

	@Override
	public void setOutputType(TypeInformation<R> outTypeInfo, ExecutionConfig executionConfig) {
		accSerializer = accTypeInformation.createSerializer(executionConfig);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);

		try {
			accSerializer.serialize(initialValue, out);
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to serialize initial value of type " +
				initialValue.getClass().getSimpleName() + " of fold window function.", ioe);
		}

		serializedInitialValue = baos.toByteArray();
	}

}
