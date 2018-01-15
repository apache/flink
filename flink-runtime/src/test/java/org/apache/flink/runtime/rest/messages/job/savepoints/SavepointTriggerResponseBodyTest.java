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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SavepointTriggerResponseBody}.
 */
public class SavepointTriggerResponseBodyTest
		extends RestResponseMarshallingTestBase<SavepointTriggerResponseBody> {

	@Override
	protected Class<SavepointTriggerResponseBody> getTestResponseClass() {
		return SavepointTriggerResponseBody.class;
	}

	@Override
	protected SavepointTriggerResponseBody getTestResponseInstance() {
		return new SavepointTriggerResponseBody(new SavepointTriggerId());
	}

	@Override
	protected void assertOriginalEqualsToUnmarshalled(
			final SavepointTriggerResponseBody expected,
			final SavepointTriggerResponseBody actual) {
		assertEquals(expected.getSavepointTriggerId(), actual.getSavepointTriggerId());
	}
}
