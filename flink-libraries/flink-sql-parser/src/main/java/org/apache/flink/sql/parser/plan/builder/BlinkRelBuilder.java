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

package org.apache.flink.sql.parser.plan.builder;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import java.util.Properties;

/**
 * Blink rel builder.
 */
public class BlinkRelBuilder extends RelBuilder {

	public BlinkRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
		super(context, cluster, relOptSchema);
	}

	public RelOptPlanner getPlanner() {
		return this.cluster.getPlanner();
	}

	public static BlinkRelBuilder create(FrameworkConfig config) {
		JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(config.getTypeSystem());

		VolcanoPlanner planner = new VolcanoPlanner(config.getCostFactory(), Contexts.empty());
		planner.setExecutor(config.getExecutor());
		planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

		RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
		CalciteSchema calciteSchema = CalciteSchema.from(config.getDefaultSchema());
		Properties prop = new Properties();
		prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
			String.valueOf(config.getParserConfig().caseSensitive()));
		CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(prop);
		RelOptSchema relOptSchema = new CalciteCatalogReader(
			calciteSchema,
			CalciteSchema.from(config.getDefaultSchema()).path(null),
			typeFactory,
			connectionConfig);

		return new BlinkRelBuilder(config.getContext(), cluster, relOptSchema);
	}
}
