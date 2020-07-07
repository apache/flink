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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.List;

/**
 * Operation to describe ALTER TABLE DROP PARTITION statement.
 */
public class DropPartitionsOperation extends AlterTableOperation {

	private final boolean ifExists;
	private final List<CatalogPartitionSpec> partitionSpecs;

	public DropPartitionsOperation(ObjectIdentifier tableIdentifier, boolean ifExists, List<CatalogPartitionSpec> partitionSpecs) {
		super(tableIdentifier);
		this.ifExists = ifExists;
		this.partitionSpecs = partitionSpecs;
	}

	public boolean ifExists() {
		return ifExists;
	}

	public List<CatalogPartitionSpec> getPartitionSpecs() {
		return partitionSpecs;
	}

	@Override
	public String asSummaryString() {
		StringBuilder builder = new StringBuilder(String.format("ALTER TABLE %s DROP", tableIdentifier.asSummaryString()));
		if (ifExists) {
			builder.append(" IF EXISTS");
		}
		for (CatalogPartitionSpec spec : partitionSpecs) {
			builder.append(String.format(" PARTITION (%s)", OperationUtils.formatPartitionSpec(spec)));
		}
		return builder.toString();
	}
}
