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

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CommonTable is the parent of table and view. A common table has a map of
 * key-value pairs defining the properties of the table. Also, any CommonTable can be
 * converted to a DataSource.
 */
public abstract class CommonTable {
	Map<String, String> tableProperties;

	public CommonTable(Map<String, String> tableProperties) {
		this.tableProperties = checkNotNull(tableProperties, "tableProperties cannot be null");
	}

	public Map<String, String> getTableProperties() {
		return tableProperties;
	}
}
