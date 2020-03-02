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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Implementation of {@link Parser} that uses Calcite.
 */
public class ParserImpl implements Parser {

	private final CatalogManager catalogManager;

	// we use supplier pattern here in order to use the most up to
	// date configuration. Users might change the parser configuration in a TableConfig in between
	// multiple statements parsing
	private final Supplier<FlinkPlannerImpl> validatorSupplier;
	private final Supplier<CalciteParser> calciteParserSupplier;

	public ParserImpl(
			CatalogManager catalogManager,
			Supplier<FlinkPlannerImpl> validatorSupplier,
			Supplier<CalciteParser> calciteParserSupplier) {
		this.catalogManager = catalogManager;
		this.validatorSupplier = validatorSupplier;
		this.calciteParserSupplier = calciteParserSupplier;
	}

	@Override
	public List<Operation> parse(String statement) {
		CalciteParser parser = calciteParserSupplier.get();
		FlinkPlannerImpl planner = validatorSupplier.get();
		// parse the sql query
		SqlNode parsed = parser.parse(statement);

		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, parsed)
			.orElseThrow(() -> new TableException("Unsupported query: " + statement));
		return Collections.singletonList(operation);
	}

	@Override
	public UnresolvedIdentifier parseIdentifier(String identifier) {
		CalciteParser parser = calciteParserSupplier.get();
		SqlIdentifier sqlIdentifier = parser.parseIdentifier(identifier);
		return UnresolvedIdentifier.of(sqlIdentifier.names);
	}
}
