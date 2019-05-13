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

package org.apache.flink.table.catalog.exceptions;

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;

/**
 * Exception for operation on a partition that doesn't exist. The cause includes non-existent table,
 * non-partitioned table, invalid partition spec, etc.
 */
public class PartitionNotExistException extends Exception {
	private static final String MSG = "Partition %s of table %s in catalog %s does not exist.";

	public PartitionNotExistException(
		String catalogName,
		ObjectPath tablePath,
		CatalogPartitionSpec partitionSpec) {

		super(String.format(MSG, partitionSpec, tablePath.getFullName(), catalogName), null);
	}

	public PartitionNotExistException(
		String catalogName,
		ObjectPath tablePath,
		CatalogPartitionSpec partitionSpec,
		Throwable cause) {

		super(String.format(MSG, partitionSpec, tablePath.getFullName(), catalogName), cause);
	}
}
