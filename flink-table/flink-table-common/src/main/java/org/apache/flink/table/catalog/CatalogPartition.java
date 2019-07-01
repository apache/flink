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
import java.util.Optional;

/**
 * Represents a partition object in catalog.
 */
public interface CatalogPartition {

	/**
	 * Get a map of properties associated with the partition.
	 *
	 * @return a map of properties with the partition
	 */
	Map<String, String> getProperties();

	/**
	 * Get comment of the partition.
	 *
	 * @return comment of the partition
	 */
	String getComment();

	/**
	 * Get a deep copy of the CatalogPartition instance.
	 *
	 * @return a copy of CatalogPartition instance
	 */
	CatalogPartition copy();

	/**
	 * Get a brief description of the database.
	 *
	 * @return an optional short description of the database
	 */
	Optional<String> getDescription();

	/**
	 * Get a detailed description of the database.
	 *
	 * @return an optional long description of the database
	 */
	Optional<String> getDetailedDescription();
}
