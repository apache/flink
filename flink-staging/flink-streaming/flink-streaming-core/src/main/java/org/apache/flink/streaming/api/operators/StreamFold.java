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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StreamFold<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, FoldFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private OUT accumulator;
	protected TypeSerializer<OUT> outTypeSerializer;
	protected TypeInformation<OUT> outTypeInformation;

	public StreamFold(FoldFunction<IN, OUT> folder, OUT initialValue, TypeInformation<OUT> outTypeInformation) {
		super(folder);
		this.accumulator = initialValue;
		this.outTypeInformation = outTypeInformation;
		this.chainingStrategy = ChainingStrategy.FORCE_ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		accumulator = userFunction.fold(outTypeSerializer.copy(accumulator), element.getValue());
		output.collect(element.replace(accumulator));
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		this.outTypeSerializer = outTypeInformation.createSerializer(executionConfig);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		output.emitWatermark(mark);
	}
}
