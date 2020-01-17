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
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_DEFAULT_PARTITIONS;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_STARTUP_MODE;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_TYPE_VALUE_PULSAR;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;


public class PulsarCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		DescriptorProperties dp = getValidateProperties(properties);
		String defaultDB = dp.getOptionalString(CATALOG_DEFAULT_DATABASE).orElse("public/default");
		String adminUrl = dp.getString(CATALOG_ADMIN_URL);

		return new PulsarCatalog(adminUrl, name, dp.asMap(), defaultDB);
	}

	@Override
	public Map<String, String> requiredContext() {
		HashMap<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR);
		context.put(CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List props = new ArrayList<String>();
		props.add(CATALOG_DEFAULT_DATABASE);
		props.add(CATALOG_PULSAR_VERSION);
		props.add(CATALOG_SERVICE_URL);
		props.add(CATALOG_ADMIN_URL);
		props.add(CATALOG_STARTUP_MODE);
		props.add(CATALOG_DEFAULT_PARTITIONS);
		return props;
	}

	private DescriptorProperties getValidateProperties(Map<String, String> properties) {
		DescriptorProperties dp = new DescriptorProperties();
		dp.putProperties(properties);
		new PulsarCatalogValidator().validate(dp);
		return dp;
	}
}
