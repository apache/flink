/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link StateUtil}.
 */
public class StateUtilTest {

	@Test
	public void unexpectedStateExceptionForSingleExpectedType() {
		Exception exception = StateUtil.unexpectedStateHandleException(
				KeyGroupsStateHandle.class,
				KeyGroupsStateHandle.class);

		assertThat(
				exception.getMessage(),
				containsString(
						"Unexpected state handle type, expected one of: class org.apache.flink.runtime.state.KeyGroupsStateHandle, but found: class org.apache.flink.runtime.state.KeyGroupsStateHandle. This can mostly happen when a different StateBackend from the one that was used for taking a checkpoint/savepoint is used when restoring."));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void unexpectedStateExceptionForMultipleExpectedTypes() {
		Exception exception = StateUtil.unexpectedStateHandleException(
				new Class[]{KeyGroupsStateHandle.class, KeyGroupsStateHandle.class},
				KeyGroupsStateHandle.class);

		assertThat(
				exception.getMessage(),
				containsString(
						"Unexpected state handle type, expected one of: class org.apache.flink.runtime.state.KeyGroupsStateHandle, class org.apache.flink.runtime.state.KeyGroupsStateHandle, but found: class org.apache.flink.runtime.state.KeyGroupsStateHandle. This can mostly happen when a different StateBackend from the one that was used for taking a checkpoint/savepoint is used when restoring."));
	}
}
