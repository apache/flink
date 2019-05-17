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

import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.expressions.FunctionDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Thin wrapper around Calcite functions, this is a temporary solution
 * that allows to register those functions in the {@link CatalogManager}.
 */
public class CalciteCatalogFunction implements CatalogFunction {

	private final FunctionDefinition functionDefinition;
	private final SqlFunction sqlFunction;

	public CalciteCatalogFunction(FunctionDefinition functionDefinition, SqlFunction sqlFunction) {
		this.functionDefinition = checkNotNull(functionDefinition);
		this.sqlFunction = checkNotNull(sqlFunction);
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	public SqlFunction getSqlFunction() {
		return sqlFunction;
	}

	@Override
	public String getClassName() {
		return null;
	}

	@Override
	public Map<String, String> getProperties() {
		return new HashMap<>();
	}

	@Override
	public CatalogFunction copy() {
		return this;
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.empty();
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.empty();
	}
}
