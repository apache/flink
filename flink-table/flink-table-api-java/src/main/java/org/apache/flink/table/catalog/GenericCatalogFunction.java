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

package org.apache.flink.table.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A generic catalog function implementation.
 */
public class GenericCatalogFunction extends AbstractCatalogFunction {

	public GenericCatalogFunction(String className) {
		this(className, new HashMap<>());
	}

	public GenericCatalogFunction(String className, Map<String, String> properties) {
		super(className, properties);
		properties.put(GenericInMemoryCatalog.FLINK_IS_GENERIC_KEY, GenericInMemoryCatalog.FLINK_IS_GENERIC_VALUE);
	}

	@Override
	public GenericCatalogFunction copy() {
		return new GenericCatalogFunction(getClassName(), new HashMap<>(getProperties()));
	}

	@Override
	public String toString() {
		return "GenericCatalogFunction{" +
			", className='" + getClassName() + '\'' +
			", properties=" + getProperties() +
			'}';
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of("This is a user-defined function");
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a user-defined function");
	}

}
