/**
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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
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

@Internal
public class FoldApplyWindowFunction<K, W extends Window, T, ACC>
	extends WrappingFunction<WindowFunction<ACC, ACC, K, W>>
	implements WindowFunction<T, ACC, K, W>, OutputTypeConfigurable<ACC> {

	private static final long serialVersionUID = 1L;

	private final FoldFunction<T, ACC> foldFunction;

	private byte[] serializedInitialValue;
	private TypeSerializer<ACC> accSerializer;
	private transient ACC initialValue;

	public FoldApplyWindowFunction(ACC initialValue, FoldFunction<T, ACC> foldFunction, WindowFunction<ACC, ACC, K, W> windowFunction) {
		super(windowFunction);
		this.foldFunction = foldFunction;
		this.initialValue = initialValue;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);

		if (serializedInitialValue == null) {
			throw new RuntimeException("No initial value was serialized for the fold " +
				"window function. Probably the setOutputType method was not called.");
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(serializedInitialValue);
		DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);
		initialValue = accSerializer.deserialize(in);
	}

	@Override
	public void apply(K key, W window, Iterable<T> values, Collector<ACC> out) throws Exception {
		ACC result = accSerializer.copy(initialValue);

		for (T val: values) {
			result = foldFunction.fold(result, val);
		}

		wrappedFunction.apply(key, window, Collections.singletonList(result), out);
	}

	@Override
	public void setOutputType(TypeInformation<ACC> outTypeInfo, ExecutionConfig executionConfig) {
		accSerializer = outTypeInfo.createSerializer(executionConfig);

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
