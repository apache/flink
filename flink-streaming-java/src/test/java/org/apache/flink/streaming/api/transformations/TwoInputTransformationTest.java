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
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link TwoInputTransformation}.
 */
public class TwoInputTransformationTest {

	@Test
	public void forwardsIsBoundedBothInputsTrue() {
		TwoInputTransformation<String, String, String> twoInputTransformation =
				createTwoInputTransform(
						createSourceTransform(true),
						createSourceTransform(true));

		assertTrue(twoInputTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedBothInputsFalse() {
		TwoInputTransformation<String, String, String> twoInputTransformation =
				createTwoInputTransform(
						createSourceTransform(false),
						createSourceTransform(false));

		assertFalse(twoInputTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedMixed() {
		TwoInputTransformation<String, String, String> twoInputTransformation1 =
				createTwoInputTransform(
						createSourceTransform(false),
						createSourceTransform(true));
		TwoInputTransformation<String, String, String> twoInputTransformation2 =
				createTwoInputTransform(
						createSourceTransform(true),
						createSourceTransform(false));

		assertFalse(twoInputTransformation1.isBounded());
		assertFalse(twoInputTransformation2.isBounded());
	}

	@Test
	public void forwardsIsBoundedMixed2() {
		TwoInputTransformation<String, String, String> twoInputTransformation =
				createTwoInputTransform(
						createSourceTransform(true),
						createSourceTransform(false));

		assertFalse(twoInputTransformation.isBounded());
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
	private static TwoInputTransformation<String, String, String> createTwoInputTransform(
			Transformation<String> input1,
			Transformation<String> input2) {
		return new TwoInputTransformation<String, String, String>(
				input1,
				input2,
				"test",
				mock(TwoInputStreamOperator.class),
				BasicTypeInfo.STRING_TYPE_INFO,
				1);
	}
}
