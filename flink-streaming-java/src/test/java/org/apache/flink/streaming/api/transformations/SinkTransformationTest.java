/*
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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link SinkTransformation}.
 */
public class SinkTransformationTest {

	@Test
	public void forwardsIsBoundedTrue() {
		SinkTransformation<String> sinkTransformation = createSinkTransform(
				createSourceTransform(true));

		assertTrue(sinkTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedFalse() {
		SinkTransformation<String> sinkTransformation = createSinkTransform(
				createSourceTransform(false));

		assertFalse(sinkTransformation.isBounded());
	}

	@SuppressWarnings("unchecked")
	private static SourceTransformation<String> createSourceTransform(boolean isBounded) {
		return new SourceTransformation<String>(
				"TestSource",
				mock(StreamSource.class),
				BasicTypeInfo.STRING_TYPE_INFO,
				1,
				isBounded);
	}

	@SuppressWarnings("unchecked")
	private static SinkTransformation<String> createSinkTransform(
			Transformation<String> input1) {
		return new SinkTransformation<String>(
				input1,
				"test",
				mock(StreamSink.class),
				1);
	}
}
