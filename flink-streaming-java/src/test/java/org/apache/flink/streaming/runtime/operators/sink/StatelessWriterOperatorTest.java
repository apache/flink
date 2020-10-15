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

package org.apache.flink.streaming.runtime.operators.sink;

/**
 * Tests for {@link StatelessWriterOperator}.
 *
 * <p>This has no tests of its own because {@link StatelessWriterOperator} has no functionality
 * beyond what {@link AbstractWriterOperator} provides.
 */
public class StatelessWriterOperatorTest extends WriterOperatorTestBase {
	@Override
	protected <InputT, CommT> AbstractWriterOperatorFactory<InputT, CommT> createWriterOperator(
			TestSink<InputT, CommT, ?, ?> sink) {
		return new StatelessWriterOperatorFactory<>(sink);
	}
}
