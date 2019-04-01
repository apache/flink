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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;

import java.util.Collections;
import java.util.List;

/**
 * Table operation that computes new table using given {@link Expression}s
 * from its input relational operation.
 */
@Internal
public class ProjectTableOperation implements TableOperation {

	private final List<Expression> projectList;
	private final TableOperation child;
	private final TableSchema tableSchema;

	public ProjectTableOperation(
			List<Expression> projectList,
			TableOperation child,
			TableSchema tableSchema) {
		this.projectList = projectList;
		this.child = child;
		this.tableSchema = tableSchema;
	}

	public List<Expression> getProjectList() {
		return projectList;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitProject(this);
	}
}
