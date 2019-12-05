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

package org.apache.flink.table.catalog.pulsar.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_PULSAR_VERSION;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_SERVICE_URL;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_ADMIN_URL;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_STARTING_POS;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_DEFAULT_PARTITIONS;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_TYPE_VALUE_PULSAR;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;


public class PulsarCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String defaultDatabase =
			descriptorProperties.getOptionalString(CATALOG_DEFAULT_DATABASE)
				.orElse("public/default");

		String adminUrl = descriptorProperties.getString(CATALOG_ADMIN_URL);

		return new PulsarCatalog(adminUrl, name, descriptorProperties.asMap(), defaultDatabase);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR);
		context.put(CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CATALOG_DEFAULT_DATABASE);
		properties.add(CATALOG_PULSAR_VERSION);
		properties.add(CATALOG_SERVICE_URL);
		properties.add(CATALOG_ADMIN_URL);
		properties.add(CATALOG_STARTING_POS);
		properties.add(CATALOG_DEFAULT_PARTITIONS);
		return properties;
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new PulsarCatalogValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
