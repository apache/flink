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

import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Operation to describe a ALTER TABLE .. RENAME to .. statement.
 */
public class AlterTableRenameOperation extends AlterTableOperation {
	private final ObjectIdentifier newTableIdentifier;

	public AlterTableRenameOperation(ObjectIdentifier tableIdentifier, ObjectIdentifier newTableIdentifier) {
		super(tableIdentifier);
		this.newTableIdentifier = newTableIdentifier;
	}

	public ObjectIdentifier getNewTableIdentifier() {
		return newTableIdentifier;
	}

	@Override
	public String asSummaryString() {
		return String.format("ALTER TABLE %s RENAME TO %s",
				tableIdentifier.asSummaryString(), newTableIdentifier.asSummaryString());
	}
}
