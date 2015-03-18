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

package org.apache.flink.streaming.api.invokable.operator;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;

public class StreamFoldInvokable<IN, OUT> extends ChainableInvokable<IN, OUT> {
	private static final long serialVersionUID = 1L;

	protected FoldFunction<IN, OUT> folder;
	protected OUT accumulator;
	protected IN nextValue;
	protected TypeSerializer<OUT> outTypeSerializer;

	public StreamFoldInvokable(FoldFunction<IN, OUT> folder, OUT initialValue, TypeInformation<OUT> outTypeInformation) {
		super(folder);
		this.folder = folder;
		this.accumulator = initialValue;
		this.outTypeSerializer = outTypeInformation.createSerializer(executionConfig);
	}

	@Override
	public void invoke() throws Exception {
		while (isRunning && readNext() != null) {
			fold();
		}
	}

	protected void fold() throws Exception {
		callUserFunctionAndLogException();

	}

	@Override
	protected void callUserFunction() throws Exception {

		nextValue = nextObject;
		accumulator = folder.fold(outTypeSerializer.copy(accumulator), nextValue);
		collector.collect(accumulator);

	}

	@Override
	public void collect(IN record) {
		if (isRunning) {
			nextObject = copy(record);
			callUserFunctionAndLogException();
		}
	}
}
