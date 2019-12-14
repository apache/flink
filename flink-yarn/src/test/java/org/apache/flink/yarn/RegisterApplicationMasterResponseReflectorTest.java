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

package org.apache.flink.yarn;

import org.apache.flink.util.TestLogger;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.yarn.YarnTestUtils.isHadoopVersionGreaterThanOrEquals;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link RegisterApplicationMasterResponseReflector}.
 */
public class RegisterApplicationMasterResponseReflectorTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(RegisterApplicationMasterResponseReflectorTest.class);

	@Mock
	private Container mockContainer;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testCallsMethodIfPresent() {
		final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector =
			new RegisterApplicationMasterResponseReflector(LOG, HasMethod.class);

		final List<Container> containersFromPreviousAttemptsUnsafe =
			registerApplicationMasterResponseReflector.getContainersFromPreviousAttemptsUnsafe(new
				HasMethod());

		assertThat(containersFromPreviousAttemptsUnsafe, hasSize(1));
	}

	@Test
	public void testDoesntCallMethodIfAbsent() {
		final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector =
			new RegisterApplicationMasterResponseReflector(LOG, HasMethod.class);

		final List<Container> containersFromPreviousAttemptsUnsafe =
			registerApplicationMasterResponseReflector.getContainersFromPreviousAttemptsUnsafe(new
				Object());

		assertThat(containersFromPreviousAttemptsUnsafe, empty());
	}

	@Test
	public void testGetMethodReflectiveHadoop22() {
		assumeTrue(
			"Method getContainersFromPreviousAttempts is not supported by Hadoop: " +
				VersionInfo.getVersion(),
			isHadoopVersionGreaterThanOrEquals(2, 2));

		final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector =
			new RegisterApplicationMasterResponseReflector(LOG);

		final Method method = registerApplicationMasterResponseReflector.getMethod();
		assertThat(method, notNullValue());
	}

	/**
	 * Class which has a method with the same signature as
	 * {@link RegisterApplicationMasterResponse#getContainersFromPreviousAttempts()}.
	 */
	private class HasMethod {

		/**
		 * Called from {@link #testCallsMethodIfPresent()}.
		 */
		@SuppressWarnings("unused")
		public List<Container> getContainersFromPreviousAttempts() {
			return Collections.singletonList(mockContainer);
		}
	}
}
