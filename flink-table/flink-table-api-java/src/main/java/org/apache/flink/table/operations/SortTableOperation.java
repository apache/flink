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
 * Expresses sort operation of rows of the underlying relational operation with given order.
 * It also allows specifying offset and number of rows to fetch from the sorted data set/stream.
 */
@Internal
public class SortTableOperation implements TableOperation {

	private final List<Expression> order;
	private final TableOperation child;
	private final int offset;
	private final int fetch;

	public SortTableOperation(
			List<Expression> order,
			TableOperation child) {
		this(order, child, -1, -1);
	}

	public SortTableOperation(List<Expression> order, TableOperation child, int offset, int fetch) {
		this.order = order;
		this.child = child;
		this.offset = offset;
		this.fetch = fetch;
	}

	public List<Expression> getOrder() {
		return order;
	}

	public TableOperation getChild() {
		return child;
	}

	public int getOffset() {
		return offset;
	}

	public int getFetch() {
		return fetch;
	}

	@Override
	public TableSchema getTableSchema() {
		return child.getTableSchema();
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitSort(this);
	}
}
