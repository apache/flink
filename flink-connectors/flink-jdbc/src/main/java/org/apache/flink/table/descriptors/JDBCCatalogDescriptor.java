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

import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_JDBC_BASE_URL;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_JDBC_PASSWORD;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_JDBC_USERNAME;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_TYPE_VALUE_JDBC;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Descriptor for {@link org.apache.flink.api.java.io.jdbc.catalog.JDBCCatalog}.
 */
public class JDBCCatalogDescriptor extends CatalogDescriptor {

	private final String defaultDatabase;
	private final String username;
	private final String pwd;
	private final String baseUrl;

	public JDBCCatalogDescriptor(String defaultDatabase, String username, String pwd, String baseUrl) {

		super(CATALOG_TYPE_VALUE_JDBC, 1);

		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase));
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(pwd));
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(baseUrl));

		this.defaultDatabase = defaultDatabase;
		this.username = username;
		this.pwd = pwd;
		this.baseUrl = baseUrl;
	}

	@Override
	protected Map<String, String> toCatalogProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		properties.putString(CATALOG_DEFAULT_DATABASE, defaultDatabase);
		properties.putString(CATALOG_JDBC_USERNAME, username);
		properties.putString(CATALOG_JDBC_PASSWORD, pwd);
		properties.putString(CATALOG_JDBC_BASE_URL, baseUrl);

		return properties.asMap();
	}
}
