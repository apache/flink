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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic catalog database implementation.
 */
public class GenericCatalogDatabase implements CatalogDatabase {
	private final Map<String, String> properties;
	private final String comment;

	public GenericCatalogDatabase(Map<String, String> properties) {
		this(properties, null);
	}

	public GenericCatalogDatabase(Map<String, String> properties, String comment) {
		this.properties = checkNotNull(properties, "properties cannot be null");
		this.comment = comment;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public String getComment() {
		return this.comment;
	}

	@Override
	public GenericCatalogDatabase copy() {
		return new GenericCatalogDatabase(new HashMap<>(properties), comment);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(comment);
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a generic catalog database stored in memory only");
	}
}
