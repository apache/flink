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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.List;
import java.util.Optional;

/**
 * Thin adapter between {@link SqlOperatorTable} and {@link FunctionCatalog}.
 */
@Internal
public class FunctionCatalogOperatorTable implements SqlOperatorTable {

	private final FunctionCatalog functionCatalog;
	private final FlinkTypeFactory typeFactory;

	public FunctionCatalogOperatorTable(
			FunctionCatalog functionCatalog,
			FlinkTypeFactory typeFactory) {
		this.functionCatalog = functionCatalog;
		this.typeFactory = typeFactory;
	}

	@Override
	public void lookupOperatorOverloads(
			SqlIdentifier opName,
			SqlFunctionCategory category,
			SqlSyntax syntax,
			List<SqlOperator> operatorList,
			SqlNameMatcher nameMatcher) {
		if (!opName.isSimple()) {
			return;
		}

		// We lookup only user functions via CatalogOperatorTable. Built in functions should
		// go through BasicOperatorTable
		if (isNotUserFunction(category)) {
			return;
		}

		String name = opName.getSimple();
		Optional<FunctionLookup.Result> candidateFunction = functionCatalog.lookupFunction(name);

		candidateFunction.flatMap(lookupResult ->
			convertToSqlFunction(category, name, lookupResult.getFunctionDefinition())
		).ifPresent(operatorList::add);
	}

	private boolean isNotUserFunction(SqlFunctionCategory category) {
		return category != null && !category.isUserDefinedNotSpecificFunction();
	}

	private Optional<SqlFunction> convertToSqlFunction(
			SqlFunctionCategory category,
			String name,
			FunctionDefinition functionDefinition) {
		if (functionDefinition instanceof AggregateFunctionDefinition) {
			return convertAggregateFunction(name, (AggregateFunctionDefinition) functionDefinition);
		} else if (functionDefinition instanceof ScalarFunctionDefinition) {
			return convertScalarFunction(name, (ScalarFunctionDefinition) functionDefinition);
		} else if (functionDefinition instanceof TableFunctionDefinition &&
				category != null &&
				category.isTableFunction()) {
			return convertTableFunction(name, (TableFunctionDefinition) functionDefinition);
		}

		return Optional.empty();
	}

	private Optional<SqlFunction> convertAggregateFunction(
			String name,
			AggregateFunctionDefinition functionDefinition) {
		SqlFunction aggregateFunction = UserDefinedFunctionUtils.createAggregateSqlFunction(
			name,
			name,
			functionDefinition.getAggregateFunction(),
			functionDefinition.getResultTypeInfo(),
			functionDefinition.getAccumulatorTypeInfo(),
			typeFactory
		);
		return Optional.of(aggregateFunction);
	}

	private Optional<SqlFunction> convertScalarFunction(String name, ScalarFunctionDefinition functionDefinition) {
		SqlFunction scalarFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
			name,
			name,
			functionDefinition.getScalarFunction(),
			typeFactory
		);
		return Optional.of(scalarFunction);
	}

	private Optional<SqlFunction> convertTableFunction(String name, TableFunctionDefinition functionDefinition) {
		SqlFunction tableFunction = UserDefinedFunctionUtils.createTableSqlFunction(
			name,
			name,
			functionDefinition.getTableFunction(),
			functionDefinition.getResultType(),
			typeFactory
		);
		return Optional.of(tableFunction);
	}

	@Override
	public List<SqlOperator> getOperatorList() {
		throw new UnsupportedOperationException("This should never be called");
	}
}
