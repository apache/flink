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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.AbstractCatalogPartition;
import org.apache.flink.table.catalog.CatalogPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A CatalogPartition implementation that represents a Partition in Hive.
 */
public class HiveCatalogPartition extends AbstractCatalogPartition {
	private final String location;

	public HiveCatalogPartition(Map<String, String> properties, String location) {
		super(properties, null);
		this.location = location;
	}

	public HiveCatalogPartition(Map<String, String> properties) {
		this(properties, null);
	}

	public String getLocation() {
		return location;
	}

	@Override
	public CatalogPartition copy() {
		return new HiveCatalogPartition(new HashMap<>(getProperties()), location);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.empty();
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.empty();
	}
}
