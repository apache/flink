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

import java.util.List;
import java.util.Map;

/**
 * Represents a table in a catalog.
 */
public interface CatalogTable extends CatalogBaseTable {
	/**
	 * Check if the table is partitioned or not.
	 *
	 * @return true if the table is partitioned; otherwise, false
	 */
	boolean isPartitioned();

	/**
	 * Get the partition keys of the table. This will be an empty set if the table is not partitioned.
	 *
	 * @return partition keys of the table
	 */
	List<String> getPartitionKeys();

	/**
	 * Returns a copy of this {@code CatalogTable} with given table options {@code options}.
	 *
	 * @return a new copy of this table with replaced table options
	 */
	CatalogTable copy(Map<String, String> options);

	/**
	 * Serializes this instance into a map of string-based properties.
	 *
	 * <p>Compared to the pure table options in {@link #getOptions()}, the map includes schema,
	 * partitioning, and other characteristics in a serialized form.
	 */
	Map<String, String> toProperties();
}
