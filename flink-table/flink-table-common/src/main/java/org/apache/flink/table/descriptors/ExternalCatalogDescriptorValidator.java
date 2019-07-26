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

import org.apache.flink.annotation.Internal;

/**
 * Validator for {@link ExternalCatalogDescriptor}.
 *
 * @deprecated use {@link CatalogDescriptorValidator} instead.
 */
@Deprecated
@Internal
public abstract class ExternalCatalogDescriptorValidator implements DescriptorValidator {

	/**
	 * Prefix for catalog-related properties.
	 */
	public static final String CATALOG = "catalog";

	/**
	 * Key for describing the type of the catalog. Usually used for factory discovery.
	 */
	public static final String CATALOG_TYPE = "catalog.type";

	/**
	 * Key for describing the property version. This property can be used for backwards
	 * compatibility in case the property format changes.
	 */
	public static final String CATALOG_PROPERTY_VERSION = "catalog.property-version";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(CATALOG_TYPE, false, 1);
		properties.validateInt(CATALOG_PROPERTY_VERSION, true, 0);
	}
}
