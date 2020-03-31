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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.operations.QueryOperation;

import java.sql.Timestamp;

/**
 * Class representing temporal table function over some history table.
 * It takes one single argument, the {@code timeAttribute}, for which it returns matching version of
 * the {@code underlyingHistoryTable}, from which this {@link TemporalTableFunction} was created.
 *
 * <p>This function shouldn't be evaluated. Instead calls to it should be rewritten by the optimiser
 * into other operators (like Temporal Table Join).
 */
@Internal
public final class TemporalTableFunctionImpl extends TemporalTableFunction {

	private final transient QueryOperation underlyingHistoryTable;
	private final transient Expression timeAttribute;
	private final transient Expression primaryKey;
	private final RowTypeInfo resultType;

	private TemporalTableFunctionImpl(
		QueryOperation underlyingHistoryTable,
			Expression timeAttribute,
			Expression primaryKey,
			RowTypeInfo resultType) {
		this.underlyingHistoryTable = underlyingHistoryTable;
		this.timeAttribute = timeAttribute;
		this.primaryKey = primaryKey;
		this.resultType = resultType;
	}

	public void eval(Timestamp row) {
		throw new IllegalStateException("This should never be called");
	}

	public Expression getTimeAttribute() {
		return timeAttribute;
	}

	public Expression getPrimaryKey() {
		return primaryKey;
	}

	@Override
	public RowTypeInfo getResultType() {
		return resultType;
	}

	public QueryOperation getUnderlyingHistoryTable() {
		if (underlyingHistoryTable == null) {
			throw new IllegalStateException("Accessing table field after planing/serialization");
		}
		return underlyingHistoryTable;
	}

	public static TemporalTableFunction create(
			QueryOperation operationTree,
			Expression timeAttribute,
			Expression primaryKey) {
		return new TemporalTableFunctionImpl(
			operationTree,
			timeAttribute,
			primaryKey,
			new RowTypeInfo(
				operationTree.getTableSchema().getFieldTypes(),
				operationTree.getTableSchema().getFieldNames()));
	}
}

