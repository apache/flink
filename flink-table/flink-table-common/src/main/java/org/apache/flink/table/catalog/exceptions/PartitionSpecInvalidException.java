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

import java.util.List;

/**
 * Exception for invalid PartitionSpec compared with partition key list of a partitioned Table.
 * For example, it is thrown when the size of PartitionSpec exceeds the size of partition key list, or
 * when the size of PartitionSpec is 'n' but its keys don't match the first 'n' keys in partition key list.
 */
public class PartitionSpecInvalidException extends Exception {
	private static final String MSG = "PartitionSpec %s does not match partition keys %s of table %s in catalog %s.";

	public PartitionSpecInvalidException(
		String catalogName,
		List<String> partitionKeys,
		ObjectPath tablePath,
		CatalogPartitionSpec partitionSpec) {

		super(String.format(MSG, partitionSpec, partitionKeys, tablePath.getFullName(), catalogName), null);
	}

	public PartitionSpecInvalidException(
		String catalogName,
		List<String> partitionKeys,
		ObjectPath tablePath,
		CatalogPartitionSpec partitionSpec,
		Throwable cause) {

		super(String.format(MSG, partitionSpec, partitionKeys, tablePath.getFullName(), catalogName), cause);
	}
}
