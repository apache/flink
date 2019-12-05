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

package org.apache.flink.table.catalog.pulsar.descriptors;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.DEFAULT_PARTITIONS;

/**
 * Validator for {@link PulsarCatalogDescriptor}.
 */
public class PulsarCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_PULSAR = "pulsar";
	public static final String CATALOG_PULSAR_VERSION = "pulsar-version";
	public static final String CATALOG_SERVICE_URL = "serviceUrl";
	public static final String CATALOG_ADMIN_URL = "adminUrl";
	public static final String CATALOG_STARTING_POS = "startingOffsets";
	public static final String CATALOG_DEFAULT_PARTITIONS = DEFAULT_PARTITIONS;

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_PULSAR, false);
		properties.validateString(CATALOG_PULSAR_VERSION, true, 1);
		properties.validateString(CATALOG_SERVICE_URL, false, 1);
		properties.validateString(CATALOG_ADMIN_URL, false, 1);
		properties.validateInt(CATALOG_DEFAULT_PARTITIONS, true, 1);
		validateStartingOffsets(properties);
	}

	private void validateStartingOffsets(DescriptorProperties properties) {
		if (properties.containsKey(CATALOG_STARTING_POS)) {
			String v = properties.getString(CATALOG_STARTING_POS);
			if (v != "earliest" && v != "latest") {
				throw new ValidationException(CATALOG_STARTING_POS + " should be either earliest or latest");
			}
		}
	}
}
