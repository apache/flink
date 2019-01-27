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

package org.apache.flink.table.api.functions;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.expressions.Expression;

import java.sql.Timestamp;

/**
 * Class representing temporal table function over some history table.
 * It takes one single argument, the {@code timeAttribute}, for which it returns matching version of
 * the {@code underlyingHistoryTable}, from which this {@link TemporalTableFunction} was created.
 *
 * <p>This function shouldn't be evaluated. Instead calls to it should be rewritten by the optimiser
 * into other operators (like Temporal Table Join).
 */
public class TemporalTableFunction extends TableFunction<BaseRow> {

	private final transient Table underlyingHistoryTable;
	private final Expression timeAttribute;
	private final String primaryKey;
	private final RowType rowType;

	private TemporalTableFunction(
		Table underlyingHistoryTable,
		Expression timeAttribute,
		String primaryKey,
		RowType rowType) {
		this.underlyingHistoryTable = underlyingHistoryTable;
		this.timeAttribute = timeAttribute;
		this.primaryKey = primaryKey;
		this.rowType = rowType;
	}

	public void eval(Timestamp row) {
		throw new IllegalStateException("This should never be called");
	}

	@Override
	public RowType getResultType(Object[] arguments, Class[] argTypes) {
		return rowType;
	}

	public Expression getTimeAttribute() {
		return timeAttribute;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public Table getUnderlyingHistoryTable() {
		if (underlyingHistoryTable == null) {
			throw new IllegalStateException("Accessing table field after planing/serialization");
		}
		return underlyingHistoryTable;
	}

	public static TemporalTableFunction create(Table table, Expression timeAttribute, String primaryKey) {
		return new TemporalTableFunction(
			table,
			timeAttribute,
			primaryKey,
			new RowType(
				table.getSchema().getTypes(),
				table.getSchema().getColumnNames()));
	}
}

