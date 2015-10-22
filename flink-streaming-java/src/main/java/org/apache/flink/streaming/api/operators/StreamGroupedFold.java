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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StreamGroupedFold<IN, OUT, KEY>
		extends AbstractUdfStreamOperator<OUT, FoldFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;
	
	private static final String STATE_NAME = "_op_state";

	// Grouped values
	private transient OperatorState<OUT> values;
	
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

		ByteArrayInputStream bais = new ByteArrayInputStream(serializedInitialValue);
		InputViewDataInputStreamWrapper in = new InputViewDataInputStreamWrapper(
				new DataInputStream(bais)
		);
		initialValue = outTypeSerializer.deserialize(in);
		values = createKeyValueState(STATE_NAME, outTypeSerializer, null);
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
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		outTypeSerializer = outTypeInfo.createSerializer(executionConfig);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputViewDataOutputStreamWrapper out = new OutputViewDataOutputStreamWrapper(
				new DataOutputStream(baos)
		);

		try {
			outTypeSerializer.serialize(initialValue, out);
		} catch (IOException ioe) {
			throw new RuntimeException("Unable to serialize initial value of type " +
					initialValue.getClass().getSimpleName() + " of fold operator.", ioe);
		}

		serializedInitialValue = baos.toByteArray();
	}

}
