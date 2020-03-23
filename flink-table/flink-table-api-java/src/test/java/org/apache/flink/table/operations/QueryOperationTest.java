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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.Test;

import java.util.Collections;

import static org.apache.flink.table.expressions.ApiExpressionUtils.intervalOfMillis;
import static org.junit.Assert.assertEquals;

/**
 * Tests for describing {@link Operation}s.
 */
public class QueryOperationTest {

	@Test
	public void testSummaryString() {
		TableSchema schema = TableSchema.builder().field("a", DataTypes.INT()).build();

		ProjectQueryOperation tableOperation = new ProjectQueryOperation(
			Collections.singletonList(new FieldReferenceExpression("a", DataTypes.INT(), 0, 0)),
			new CatalogQueryOperation(
				ObjectIdentifier.of("cat1", "db1", "tab1"),
				schema), schema);

		SetQueryOperation unionQueryOperation = new SetQueryOperation(
			tableOperation,
			tableOperation,
			SetQueryOperation.SetQueryOperationType.UNION,
			true);

		assertEquals("Union: (all: [true])\n" +
			"    Project: (projections: [a])\n" +
			"        CatalogTable: (identifier: [`cat1`.`db1`.`tab1`], fields: [a])\n" +
			"    Project: (projections: [a])\n" +
			"        CatalogTable: (identifier: [`cat1`.`db1`.`tab1`], fields: [a])",
			unionQueryOperation.asSummaryString());
	}

	@Test
	public void testWindowAggregationSummaryString() {
		TableSchema schema = TableSchema.builder().field("a", DataTypes.INT()).build();
		FieldReferenceExpression field = new FieldReferenceExpression("a", DataTypes.INT(), 0, 0);
		WindowAggregateQueryOperation tableOperation = new WindowAggregateQueryOperation(
			Collections.singletonList(field),
			Collections.singletonList(
				new CallExpression(BuiltInFunctionDefinitions.SUM, Collections.singletonList(field), DataTypes.INT())),
			Collections.emptyList(),
			WindowAggregateQueryOperation.ResolvedGroupWindow.sessionWindow("w", field, intervalOfMillis(10)),
			new CatalogQueryOperation(
				ObjectIdentifier.of("cat1", "db1", "tab1"),
				schema),
			schema
		);

		DistinctQueryOperation distinctQueryOperation = new DistinctQueryOperation(tableOperation);

		assertEquals(
			"Distinct:\n" +
			"    WindowAggregate: (group: [a], agg: [sum(a)], windowProperties: []," +
				" window: [SessionWindow(field: [a], gap: [10])])\n" +
				"        CatalogTable: (identifier: [`cat1`.`db1`.`tab1`], fields: [a])",
			distinctQueryOperation.asSummaryString());
	}

	@Test
	public void testIndentation() {

		String input =
			"firstLevel\n" +
			"    secondLevel0\n" +
			"        thirdLevel0\n" +
			"    secondLevel1\n" +
			"        thirdLevel1";

		String indentedInput = OperationUtils.indent(input);

		assertEquals(
			"\n" +
			"    firstLevel\n" +
			"        secondLevel0\n" +
			"            thirdLevel0\n" +
			"        secondLevel1\n" +
			"            thirdLevel1",
			indentedInput);
	}
}
