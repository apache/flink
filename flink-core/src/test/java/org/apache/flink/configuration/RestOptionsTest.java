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

package org.apache.flink.configuration;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link RestOptions}.
 */
public class RestOptionsTest extends TestLogger {

	@Test
	public void testBindAddressFirstDeprecatedKey() {
		final Configuration configuration = new Configuration();
		final String expectedAddress = "foobar";
		configuration.setString("web.address", expectedAddress);

		final String actualAddress = configuration.getString(RestOptions.BIND_ADDRESS);

		assertThat(actualAddress, is(equalTo(expectedAddress)));
	}

	@Test
	public void testBindAddressSecondDeprecatedKey() {
		final Configuration configuration = new Configuration();
		final String expectedAddress = "foobar";
		configuration.setString("jobmanager.web.address", expectedAddress);

		final String actualAddress = configuration.getString(RestOptions.BIND_ADDRESS);

		assertThat(actualAddress, is(equalTo(expectedAddress)));
	}
}
