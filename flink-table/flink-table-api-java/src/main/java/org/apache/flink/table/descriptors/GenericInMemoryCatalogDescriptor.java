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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;

/**
 * Catalog descriptor for the generic in memory catalog.
 */
public class GenericInMemoryCatalogDescriptor extends CatalogDescriptor {

	public GenericInMemoryCatalogDescriptor() {
		super(CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY, 1);
	}

	public GenericInMemoryCatalogDescriptor(String defaultDatabase) {
		super(CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY, 1, defaultDatabase);
	}

	@Override
	protected Map<String, String> toCatalogProperties() {
		return Collections.unmodifiableMap(new HashMap<>());
	}
}
