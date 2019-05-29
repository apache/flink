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

import org.apache.flink.table.catalog.AbstractCatalogDatabase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A hive catalog database implementation.
 */
public class HiveCatalogDatabase extends AbstractCatalogDatabase {
	// HDFS path of the database
	private final String location;

	public HiveCatalogDatabase(Map<String, String> properties, String comment) {
		super(properties, comment);
		location = null;
	}

	public HiveCatalogDatabase(Map<String, String> properties, String location, String comment) {
		super(properties, comment);
		this.location = location;
	}

	@Override
	public HiveCatalogDatabase copy() {
		return new HiveCatalogDatabase(new HashMap<>(getProperties()), location, getComment());
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.ofNullable(getComment());
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a Hive catalog database");
	}

	public String getLocation() {
		return location;
	}

}
