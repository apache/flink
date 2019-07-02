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
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link CoFeedbackTransformation}.
 */
public class CoFeedbackTransformationTest {

	@Test
	public void forwardsIsBoundedAllTrue() {
		CoFeedbackTransformation<String> coFeedbackTransformation = createCoFeedbackTransform();

		coFeedbackTransformation.addFeedbackEdge(createTestSourceTransform(true));
		coFeedbackTransformation.addFeedbackEdge(createTestSourceTransform(true));

		assertTrue(coFeedbackTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedAllFalse() {
		CoFeedbackTransformation<String> coFeedbackTransformation = createCoFeedbackTransform();

		coFeedbackTransformation.addFeedbackEdge(createTestSourceTransform(false));
		coFeedbackTransformation.addFeedbackEdge(createTestSourceTransform(false));

		assertFalse(coFeedbackTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedMixed() {
		CoFeedbackTransformation<String> coFeedbackTransformation = createCoFeedbackTransform();

		coFeedbackTransformation.addFeedbackEdge(createTestSourceTransform(true));
		coFeedbackTransformation.addFeedbackEdge(createTestSourceTransform(false));

		assertFalse(coFeedbackTransformation.isBounded());
	}

	@SuppressWarnings("unchecked")
	private static SourceTransformation<String> createTestSourceTransform(boolean isBounded) {
		return new SourceTransformation<String>(
				"TestSource",
				mock(StreamSource.class),
				BasicTypeInfo.STRING_TYPE_INFO,
				1,
				isBounded);
	}

	private static CoFeedbackTransformation<String> createCoFeedbackTransform() {
		return new CoFeedbackTransformation<>(1, BasicTypeInfo.STRING_TYPE_INFO, 0L);
	}
}
