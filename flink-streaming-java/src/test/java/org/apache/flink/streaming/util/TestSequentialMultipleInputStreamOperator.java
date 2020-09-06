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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * A test operator class for sequential reading.
 */
public class TestSequentialMultipleInputStreamOperator extends TestAnyModeMultipleInputStreamOperator
	implements BoundedMultiInput {

	private InputSelection inputSelection = InputSelection.FIRST;

	public TestSequentialMultipleInputStreamOperator(StreamOperatorParameters<String> parameters) {
		super(parameters);
	}

	@Override
	public InputSelection nextSelection() {
		return inputSelection;
	}

	@Override
	public void endInput(int inputId) {
		if (inputId == 1) {
			inputSelection = InputSelection.SECOND;
		}
	}

	/**
	 * Factory to construct {@link TestSequentialMultipleInputStreamOperator}.
	 */
	public static class Factory extends AbstractStreamOperatorFactory<String> {

		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new TestSequentialMultipleInputStreamOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return TestSequentialMultipleInputStreamOperator.class;
		}
	}
}
