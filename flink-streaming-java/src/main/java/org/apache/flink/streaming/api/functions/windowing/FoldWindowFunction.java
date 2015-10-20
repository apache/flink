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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FoldWindowFunction<K, W extends Window, T, R>
		extends WrappingFunction<FoldFunction<T, R>>
		implements WindowFunction<T, R, K, W>, OutputTypeConfigurable<R> {
	private static final long serialVersionUID = 1L;

	private byte[] serializedInitialValue;
	private TypeSerializer<R> outSerializer;
	private transient R initialValue;

	public FoldWindowFunction(R initialValue, FoldFunction<T, R> reduceFunction) {
		super(reduceFunction);
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
		InputViewDataInputStreamWrapper in = new InputViewDataInputStreamWrapper(
				new DataInputStream(bais)
		);
		initialValue = outSerializer.deserialize(in);
	}

	@Override
	public void apply(K k, W window, Iterable<T> values, Collector<R> out) throws Exception {
		R result = outSerializer.copy(initialValue);

		for (T val: values) {
			result = wrappedFunction.fold(result, val);
		}

		out.collect(result);
	}

	@Override
	public void setOutputType(TypeInformation<R> outTypeInfo, ExecutionConfig executionConfig) {
		outSerializer = outTypeInfo.createSerializer(executionConfig);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputViewDataOutputStreamWrapper out = new OutputViewDataOutputStreamWrapper(
				new DataOutputStream(baos)
		);

		try {
			outSerializer.serialize(initialValue, out);
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to serialize initial value of type " +
					initialValue.getClass().getSimpleName() + " of fold window function.", ioe);
		}

		serializedInitialValue = baos.toByteArray();
	}
}
