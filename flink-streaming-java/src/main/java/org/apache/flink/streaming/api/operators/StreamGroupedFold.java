/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A {@link StreamOperator} for executing a {@link FoldFunction} on a
 * {@link org.apache.flink.streaming.api.datastream.KeyedStream}.
 *
 * @deprecated will be removed in a future version
 */
@Internal
@Deprecated
public class StreamGroupedFold<IN, OUT, KEY>
		extends AbstractUdfStreamOperator<OUT, FoldFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;

	private static final String STATE_NAME = "_op_state";

	// Grouped values
	private transient ValueState<OUT> values;

	private transient OUT initialValue;

	// Initial value serialization
	private byte[] serializedInitialValue;

	private TypeSerializer<OUT> outTypeSerializer;

	public StreamGroupedFold(FoldFunction<IN, OUT> folder, OUT initialValue) {
		super(folder);
		this.initialValue = initialValue;
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (serializedInitialValue == null) {
			throw new RuntimeException("No initial value was serialized for the fold " +
					"operator. Probably the setOutputType method was not called.");
		}

		try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedInitialValue);
			DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {
			initialValue = outTypeSerializer.deserialize(in);
		}

		ValueStateDescriptor<OUT> stateId = new ValueStateDescriptor<>(STATE_NAME, outTypeSerializer);
		values = getPartitionedState(stateId);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		OUT value = values.value();

		if (value != null) {
			OUT folded = userFunction.fold(outTypeSerializer.copy(value), element.getValue());
			values.update(folded);
			output.collect(element.replace(folded));
		} else {
			OUT first = userFunction.fold(outTypeSerializer.copy(initialValue), element.getValue());
			values.update(first);
			output.collect(element.replace(first));
		}
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		outTypeSerializer = outTypeInfo.createSerializer(executionConfig);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);

		try {
			outTypeSerializer.serialize(initialValue, out);
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to serialize initial value of type " +
					initialValue.getClass().getSimpleName() + " of fold operator.", ioe);
		}

		serializedInitialValue = baos.toByteArray();
	}

}
