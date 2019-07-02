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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link FeedbackTransformation}.
 */
public class FeedbackTransformationTest {

	@Test
	public void forwardsIsBoundedTrue() {
		FeedbackTransformation<String> feedbackTransformation = createFeedbackTransform(
				createSourceTransform(true));

		assertTrue(feedbackTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedFalse() {
		FeedbackTransformation<String> feedbackTransformation = createFeedbackTransform(
				createSourceTransform(false));

		assertFalse(feedbackTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedTrueFeedbackFalse() {
		FeedbackTransformation<String> feedbackTransformation = createFeedbackTransform(
				createSourceTransform(true));

		feedbackTransformation.addFeedbackEdge(createSourceTransform(false));

		assertFalse(feedbackTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedFalseFeedbackTrue() {
		FeedbackTransformation<String> feedbackTransformation = createFeedbackTransform(
				createSourceTransform(false));

		feedbackTransformation.addFeedbackEdge(createSourceTransform(true));

		assertFalse(feedbackTransformation.isBounded());
	}

	@Test
	public void forwardsIsBoundedTrueFeedbackMixed() {
		FeedbackTransformation<String> feedbackTransformation = createFeedbackTransform(
				createSourceTransform(true));

		feedbackTransformation.addFeedbackEdge(createSourceTransform(true));
		feedbackTransformation.addFeedbackEdge(createSourceTransform(false));

		assertFalse(feedbackTransformation.isBounded());
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

	private static FeedbackTransformation<String> createFeedbackTransform(
			Transformation<String> input) {
		return new FeedbackTransformation<>(input, 0L);
	}
}
