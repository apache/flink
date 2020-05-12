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

package org.apache.flink.table.descriptors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_TYPE_VALUE_JDBC;

/**
 * Test for {@link JdbcCatalogDescriptor}.
 */
public class JdbcCatalogDescriptorTest extends DescriptorTestBase {

	private static final String TEST_DB = "db";
	private static final String TEST_USERNAME = "user";
	private static final String TEST_PWD = "pwd";
	private static final String TEST_BASE_URL = "xxx";

	@Override
	protected List<Descriptor> descriptors() {
		final Descriptor descriptor = new JdbcCatalogDescriptor(
			TEST_DB, TEST_USERNAME, TEST_PWD, TEST_BASE_URL);

		return Arrays.asList(descriptor);
	}

	@Override
	protected List<Map<String, String>> properties() {
		final Map<String, String> props = new HashMap<>();
		props.put("type", CATALOG_TYPE_VALUE_JDBC);
		props.put("property-version", "1");
		props.put("default-database", TEST_DB);
		props.put("username", TEST_USERNAME);
		props.put("password", TEST_PWD);
		props.put("base-url", TEST_BASE_URL);

		return Arrays.asList(props);
	}

	@Override
	protected DescriptorValidator validator() {
		return new JdbcCatalogValidator();
	}
}
