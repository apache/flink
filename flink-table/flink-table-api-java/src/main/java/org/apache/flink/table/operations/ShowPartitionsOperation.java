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

package org.apache.flink.table.operations;

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Operation to describe a SHOW PARTITIONS statement.
 */
public class ShowPartitionsOperation implements ShowOperation {

	protected final ObjectIdentifier tableIdentifier;
	private final CatalogPartitionSpec partitionSpec;

	public ShowPartitionsOperation(ObjectIdentifier tableIdentifier, CatalogPartitionSpec partitionSpec) {
		this.tableIdentifier = tableIdentifier;
		this.partitionSpec = partitionSpec;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public CatalogPartitionSpec getPartitionSpec() {
		return partitionSpec;
	}

	@Override
	public String asSummaryString() {
		StringBuilder builder = new StringBuilder(String.format("SHOW PARTITIONS %s", tableIdentifier.asSummaryString()));
		if (partitionSpec != null) {
			builder.append(String.format(" PARTITION (%s)", OperationUtils.formatPartitionSpec(partitionSpec)));
		}
		return builder.toString();
	}
}
