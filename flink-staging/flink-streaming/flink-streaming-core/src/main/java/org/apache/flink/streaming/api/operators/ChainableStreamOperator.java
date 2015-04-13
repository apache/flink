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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.Collector;

public abstract class ChainableStreamOperator<IN, OUT> extends StreamOperator<IN, OUT> implements
		Collector<IN> {

	private static final long serialVersionUID = 1L;
	private boolean copyInput = true;

	public ChainableStreamOperator(Function userFunction) {
		super(userFunction);
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	public void setup(Collector<OUT> collector, StreamRecordSerializer<IN> inSerializer) {
		this.collector = collector;
		this.inSerializer = inSerializer;
		this.objectSerializer = inSerializer.getObjectSerializer();
	}

	public ChainableStreamOperator<IN, OUT> withoutInputCopy() {
		copyInput = false;
		return this;
	}

	protected IN copyInput(IN input) {
		return copyInput ? copy(input) : input;
	}

	@Override
	public void collect(IN record) {
		if (isRunning) {
			nextObject = copyInput(record);
			callUserFunctionAndLogException();
		}
	}
}
