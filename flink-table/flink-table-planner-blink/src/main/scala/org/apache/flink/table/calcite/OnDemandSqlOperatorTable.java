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

package org.apache.flink.table.calcite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.flink.table.catalog.CalciteCatalogFunction;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A SqlOperatorTable implementation that looks up {@link SqlOperator operators} on demand.
 */
public class OnDemandSqlOperatorTable implements SqlOperatorTable {

	private final CatalogManager catalogManager;

	public OnDemandSqlOperatorTable(CatalogManager catalogManager) {
		this.catalogManager = checkNotNull(catalogManager, "catalogManager cannot be null");
	}

	@Override
	public void lookupOperatorOverloads(
			SqlIdentifier sqlIdentifier,
			SqlFunctionCategory sqlFunctionCategory,
			SqlSyntax sqlSyntax,
			List<SqlOperator> operatorList) {
		if (sqlSyntax != SqlSyntax.FUNCTION) {
			return;
		}

		if (!sqlIdentifier.isSimple()) {
			return;
		}

		if (sqlFunctionCategory == null || !sqlFunctionCategory.isUserDefinedNotSpecificFunction()) {
			return;
		}

		if (sqlIdentifier.names.size() > 1) {
			throw new UnsupportedOperationException(
				String.format("OnDemandSqlOperatorTable does not support function with name space yet. " +
					"The function name is '%s'", sqlIdentifier.names));
		}

		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();

		ObjectPath functionPath =
			new ObjectPath(catalogManager.getCurrentDatabase(), sqlIdentifier.names.get(0));

		try {
			CatalogFunction catalogFunction = catalog.getFunction(functionPath);
			SqlFunction sqlFunction = null; // TODO: convert a CatalogFunction to SqlFunction

			operatorList.add(sqlFunction);

		} catch (FunctionNotExistException e) {
			// ignore
		}
	}

	/**
	 * Convert [[CatalogFunction]] to calcite SqlFunction.
	 * note: tableEnvironment should not be null
	 */
	private SqlFunction toSqlFunction(
		Catalog catalog,
		String functionName,
		CatalogFunction catalogFunction,
		FlinkTypeFactory typeFactory)
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {

		if (catalogFunction instanceof CalciteCatalogFunction) {
			return ((CalciteCatalogFunction) catalogFunction).getSqlFunction();
		} else if (catalogFunction instanceof HiveCatalogFunction) {
			// Convert HiveCatalogFunction to UserDefinedFunction
		} else if (catalogFunction instanceof CatalogFunction) {
			// Regular CatalogFunction
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		UserDefinedFunction udf = (UserDefinedFunction) classLoader.loadClass(catalogFunction.getClassName()).newInstance();

			if (udf instanceof ScalarFunction) {
				return UserDefinedFunctionUtils.createScalarSqlFunction(
					functionName,
					functionName,
					(ScalarFunction) udf,
					typeFactory);
			} else if (udf instanceof TableFunction) {
				return UserDefinedFunctionUtils.createTableSqlFunction(
					functionName,
					functionName,
					(TableFunction) udf,
					typeFactory);
			} else if (udf instanceof AggregateFunction) {
				AggregateFunction agg = (AggregateFunction) udf;
				return UserDefinedFunctionUtils.createAggregateSqlFunction(
					functionName,
					functionName,
					agg,
					UserDefinedFunctionUtils.getResultTypeOfAggregateFunction(agg, null),
					UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction(agg, null),
					typeFactory
				);
			} else {
				throw new UnsupportedOperationException(
					String.format("Function %s should be of ScalarFunction, TableFunction, or AggregateFunction", functionName));
			}
		}
	}

	@Override
	public List<SqlOperator> getOperatorList() {
		throw new UnsupportedOperationException(
			"getOperatorList() is designed for automated testing and shouldn't be called.");
	}
}
