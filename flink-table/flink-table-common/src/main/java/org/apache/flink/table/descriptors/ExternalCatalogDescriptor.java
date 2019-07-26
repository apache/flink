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

import java.util.Map;

import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_TYPE;

/**
 * Describes an external catalog of tables, views, and functions.
 *
 * @deprecated use {@link CatalogDescriptor} instead.
 */
@Deprecated
public abstract class ExternalCatalogDescriptor extends DescriptorBase implements Descriptor {

	private final String type;

	private final int version;

	/**
	 * Constructs a {@link ExternalCatalogDescriptor}.
	 *
	 * @param type string that identifies this catalog
	 * @param version property version for backwards compatibility
	 */
	public ExternalCatalogDescriptor(String type, int version) {
		this.type = type;
		this.version = version;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(CATALOG_TYPE, type);
		properties.putLong(CATALOG_PROPERTY_VERSION, version);
		properties.putProperties(toCatalogProperties());
		return properties.asMap();
	}

	/**
	 * Converts this descriptor into a set of catalog properties. Usually prefixed with
	 * {@link ExternalCatalogDescriptorValidator#CATALOG}.
	 */
	protected abstract Map<String, String> toCatalogProperties();
}
