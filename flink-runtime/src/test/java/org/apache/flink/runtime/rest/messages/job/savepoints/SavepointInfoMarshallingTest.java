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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Marshalling tests for the {@link SavepointInfo}.
 */
@RunWith(Parameterized.class)
public class SavepointInfoMarshallingTest extends RestResponseMarshallingTestBase<SavepointInfo> {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{new SavepointInfo(
				"/tmp",
				null
			)},
			{new SavepointInfo(
				null,
				new SerializedThrowable(new RuntimeException("expected")))}});
	}

	private final SavepointInfo savepointInfo;

	public SavepointInfoMarshallingTest(SavepointInfo savepointInfo) {
		this.savepointInfo = savepointInfo;
	}

	@Override
	protected Class<SavepointInfo> getTestResponseClass() {
		return SavepointInfo.class;
	}

	@Override
	protected SavepointInfo getTestResponseInstance() throws Exception {
		return savepointInfo;
	}

	@Override
	protected void assertOriginalEqualsToUnmarshalled(SavepointInfo expected, SavepointInfo actual) {
		assertThat(actual.getLocation(), is(expected.getLocation()));
		if (expected.getFailureCause() != null) {
			assertThat(actual.getFailureCause(), notNullValue());
			assertThat(
				actual.getFailureCause().deserializeError(ClassLoader.getSystemClassLoader()).getMessage(),
				is(expected.getFailureCause().deserializeError(ClassLoader.getSystemClassLoader()).getMessage()));
		}
	}
}
