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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describes a catalog of tables, views, and functions.
 */
@PublicEvolving
public abstract class CatalogDescriptor extends DescriptorBase {

	private final String type;

	private final int propertyVersion;

	private final String defaultDatabase;

	/**
	 * Constructs a {@link CatalogDescriptor}.
	 *
	 * @param type string that identifies this catalog
	 * @param propertyVersion property version for backwards compatibility
	 */
	public CatalogDescriptor(String type, int propertyVersion) {
		this(type, propertyVersion, null);
	}

	/**
	 * Constructs a {@link CatalogDescriptor}.
	 *
	 * @param type string that identifies this catalog
	 * @param propertyVersion property version for backwards compatibility
	 * @param defaultDatabase default database of the catalog
	 */
	public CatalogDescriptor(String type, int propertyVersion, String defaultDatabase) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(type), "type cannot be null or empty");

		this.type = type;
		this.propertyVersion = propertyVersion;
		this.defaultDatabase = defaultDatabase;
	}

	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putString(CATALOG_TYPE, type);
		properties.putLong(CATALOG_PROPERTY_VERSION, propertyVersion);

		if (defaultDatabase != null) {
			properties.putString(CATALOG_DEFAULT_DATABASE, defaultDatabase);
		}

		properties.putProperties(toCatalogProperties());
		return properties.asMap();
	}

	public String getDefaultDatabase() {
		return defaultDatabase;
	}

	/**
	 * Converts this descriptor into a set of catalog properties.
	 */
	protected abstract Map<String, String> toCatalogProperties();
}
