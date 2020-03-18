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

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Arrays;
import java.util.List;

/**
 * A test operator class implementing {@link BoundedMultiInput}.
 */
public class TestBoundedMultipleInputOperator extends AbstractStreamOperator<String>
	implements MultipleInputStreamOperator<String>, BoundedMultiInput {

	private static final long serialVersionUID = 1L;

	private final String name;

	public TestBoundedMultipleInputOperator(String name) {
		this.name = name;
	}

	@Override
	public List<Input> getInputs() {
		return Arrays.asList(
			new TestInput(1),
			new TestInput(2),
			new TestInput(3)
		);
	}

	@Override
	public void endInput(int inputId) {
		output.collect(new StreamRecord<>("[" + name + "-" + inputId + "]: End of input"));
	}

	@Override
	public void close() throws Exception {
		output.collect(new StreamRecord<>("[" + name + "]: Bye"));
		super.close();
	}

	class TestInput implements Input<String> {
		private final int inputIndex;

		public TestInput(int inputIndex) {
			this.inputIndex = inputIndex;
		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			output.collect(element.replace("[" + name + "-" + inputIndex + "]: " + element.getValue()));
		}
	}
}
