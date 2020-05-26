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

package org.apache.flink.table.planner.plan;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.TestTableSource;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test for FlinkCalciteCatalogReader.
 */
public class FlinkCalciteCatalogReaderTest {
	private final FlinkTypeFactory typeFactory = new FlinkTypeFactory(new FlinkTypeSystem());
	private final String tableMockName = "ts";

	private SchemaPlus rootSchemaPlus;
	private FlinkCalciteCatalogReader catalogReader;

	@Before
	public void init() {
		rootSchemaPlus = CalciteSchema.createRootSchema(true, false).plus();
		Properties prop = new Properties();
		prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
		CalciteConnectionConfigImpl calciteConnConfig = new CalciteConnectionConfigImpl(prop);
		catalogReader = new FlinkCalciteCatalogReader(
			CalciteSchema.from(rootSchemaPlus),
			Collections.emptyList(),
			typeFactory,
			calciteConnConfig);
	}

	@Test
	public void testGetFlinkPreparingTableBase() {
		// Mock CatalogSchemaTable.
		CatalogSchemaTable mockTable = new CatalogSchemaTable(
			ObjectIdentifier.of("a", "b", "c"),
			ConnectorCatalogTable.source(
				new TestTableSource(true, TableSchema.builder().build()),
				true),
			FlinkStatistic.UNKNOWN(),
			null,
			tableRowType -> new SqlExprToRexConverter() {
				@Override
				public RexNode convertToRexNode(String expr) {
					return null;
				}

				@Override
				public RexNode[] convertToRexNodes(String[] exprs) {
					return new RexNode[0];
				}
			},
			true,
			false);

		rootSchemaPlus.add(tableMockName, mockTable);
		Prepare.PreparingTable preparingTable = catalogReader
			.getTable(Collections.singletonList(tableMockName));
		assertTrue(preparingTable instanceof FlinkPreparingTableBase);
	}

	@Test
	public void testGetNonFlinkPreparingTableBase() {
		Table nonFlinkTableMock = mock(Table.class);
		when(nonFlinkTableMock.getRowType(typeFactory)).thenReturn(mock(RelDataType.class));
		rootSchemaPlus.add(tableMockName, nonFlinkTableMock);
		Prepare.PreparingTable resultTable = catalogReader
			.getTable(Collections.singletonList(tableMockName));
		assertFalse(resultTable instanceof FlinkPreparingTableBase);
	}
}
