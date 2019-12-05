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

import org.apache.flink.table.catalog.pulsar.PulsarCatalog;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_TYPE_VALUE_PULSAR;
import static org.apache.flink.table.catalog.pulsar.descriptors.PulsarCatalogValidator.CATALOG_PULSAR_VERSION;

/**
 * Catalog descriptor for {@link PulsarCatalog}.
 */
public class PulsarCatalogDescriptor extends CatalogDescriptor {

	private String pulsarVersion;

	public PulsarCatalogDescriptor() {
		super(CATALOG_TYPE_VALUE_PULSAR, 1, "public/default");
	}

	public PulsarCatalogDescriptor pulsarVersion(String version) {
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(version));
		this.pulsarVersion = version;
		return this;
	}

	@Override
	protected Map<String, String> toCatalogProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		if (pulsarVersion != null) {
			properties.putString(CATALOG_PULSAR_VERSION, pulsarVersion);
		}

		return properties.asMap();
	}
}
