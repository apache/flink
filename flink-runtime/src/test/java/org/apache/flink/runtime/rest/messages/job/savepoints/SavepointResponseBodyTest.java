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
import org.apache.flink.util.SerializedThrowable;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link SavepointResponseBody}.
 */
@RunWith(Parameterized.class)
public class SavepointResponseBodyTest extends RestResponseMarshallingTestBase<SavepointResponseBody> {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{SavepointResponseBody.inProgress()},
			{SavepointResponseBody.completed(new SavepointInfo(
				new SavepointTriggerId(),
				"/tmp",
				null))},
			{SavepointResponseBody.completed(new SavepointInfo(
				new SavepointTriggerId(),
				null,
				new SerializedThrowable(new RuntimeException("expected"))))}
		});
	}

	private final SavepointResponseBody savepointResponseBody;

	public SavepointResponseBodyTest(final SavepointResponseBody savepointResponseBody) {
		this.savepointResponseBody = savepointResponseBody;
	}

	@Override
	protected Class<SavepointResponseBody> getTestResponseClass() {
		return SavepointResponseBody.class;
	}

	@Override
	protected SavepointResponseBody getTestResponseInstance() throws Exception {
		return savepointResponseBody;
	}

	@Override
	protected void assertOriginalEqualsToUnmarshalled(
			final SavepointResponseBody expected,
			final SavepointResponseBody actual) {
		assertEquals(expected.getStatus().getId(), actual.getStatus().getId());
		if (expected.getSavepoint() != null) {
			assertNotNull(actual.getSavepoint());
			assertEquals(expected.getSavepoint().getRequestId(), actual.getSavepoint().getRequestId());
			assertEquals(expected.getSavepoint().getLocation(), actual.getSavepoint().getLocation());
			if (expected.getSavepoint().getFailureCause() != null) {
				assertNotNull(actual.getSavepoint().getFailureCause());
				assertEquals(expected.getSavepoint()
						.getFailureCause()
						.deserializeError(ClassLoader.getSystemClassLoader())
						.getMessage(),
					actual.getSavepoint()
						.getFailureCause()
						.deserializeError(ClassLoader.getSystemClassLoader())
						.getMessage());
			}
		}
	}
}
