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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.plan.metadata.MetadataTestUtil;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test for {@link ExpressionConverter}.
 */
public class ExpressionConverterTest {

	private final TableConfig tableConfig = new TableConfig();
	private final CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
	private final PlannerContext plannerContext = new PlannerContext(
			tableConfig,
			new FunctionCatalog(tableConfig, catalogManager, new ModuleManager()),
			catalogManager,
			CalciteSchema.from(MetadataTestUtil.initRootSchema()),
			Arrays.asList(
					ConventionTraitDef.INSTANCE,
					FlinkRelDistributionTraitDef.INSTANCE(),
					RelCollationTraitDef.INSTANCE
			)
	);
	private final ExpressionConverter converter = new ExpressionConverter(
		plannerContext.createRelBuilder(
			CatalogManagerMocks.DEFAULT_CATALOG,
			CatalogManagerMocks.DEFAULT_DATABASE));

	@Test
	public void testLiteral() {
		RexNode rex = converter.visit(new ValueLiteralExpression((byte) 1, DataTypes.TINYINT()));
		Assert.assertEquals(1, (int) ((RexLiteral) rex).getValueAs(Integer.class));
		Assert.assertEquals(SqlTypeName.TINYINT, rex.getType().getSqlTypeName());

		rex = converter.visit(new ValueLiteralExpression((short) 1, DataTypes.SMALLINT()));
		Assert.assertEquals(1, (int) ((RexLiteral) rex).getValueAs(Integer.class));
		Assert.assertEquals(SqlTypeName.SMALLINT, rex.getType().getSqlTypeName());

		rex = converter.visit(new ValueLiteralExpression(1, DataTypes.INT()));
		Assert.assertEquals(1, (int) ((RexLiteral) rex).getValueAs(Integer.class));
		Assert.assertEquals(SqlTypeName.INTEGER, rex.getType().getSqlTypeName());
	}
}
