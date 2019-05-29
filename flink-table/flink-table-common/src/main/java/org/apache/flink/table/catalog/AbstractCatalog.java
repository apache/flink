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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This interface is responsible for reading and writing metadata such as database/table/views/UDFs
 * from a registered catalog. It connects a registered catalog and Flink's Table API.
 */
@PublicEvolving
public abstract class AbstractCatalog implements Catalog {
	private final String catalogName;
	private final String defaultDatabase;

	public AbstractCatalog(String catalogName, String defaultDatabase) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(defaultDatabase), "defaultDatabase cannot be null or empty");

		this.catalogName = catalogName;
		this.defaultDatabase = defaultDatabase;
	}

	public String getCatalogName() {
		return catalogName;
	}

	@Override
	public String getDefaultDatabase() {
		return defaultDatabase;
	}
}
