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

package org.apache.flink.api.java.io.jdbc.catalog.factory;

import org.apache.flink.api.java.io.jdbc.catalog.JDBCCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JDBCCatalogValidator;
import org.apache.flink.table.factories.CatalogFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_JDBC_BASE_URL;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_JDBC_PASSWORD;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_JDBC_USERNAME;
import static org.apache.flink.table.descriptors.JDBCCatalogValidator.CATALOG_TYPE_VALUE_JDBC;

/**
 * Factory for {@link org.apache.flink.api.java.io.jdbc.catalog.JDBCCatalog}.
 */
public class JDBCCatalogFactory implements CatalogFactory {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCCatalogFactory.class);

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC); // jdbc
		context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// default database
		properties.add(CATALOG_DEFAULT_DATABASE);

		properties.add(CATALOG_JDBC_BASE_URL);
		properties.add(CATALOG_JDBC_USERNAME);
		properties.add(CATALOG_JDBC_PASSWORD);

		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties prop = getValidatedProperties(properties);

		return new JDBCCatalog(
			name,
			prop.getString(CATALOG_DEFAULT_DATABASE),
			prop.getString(CATALOG_JDBC_USERNAME),
			prop.getString(CATALOG_JDBC_PASSWORD),
			prop.getString(CATALOG_JDBC_BASE_URL));
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new JDBCCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

}
