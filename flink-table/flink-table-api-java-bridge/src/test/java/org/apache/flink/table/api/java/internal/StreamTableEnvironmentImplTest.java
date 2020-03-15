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

package org.apache.flink.table.api.java.internal;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.utils.ExecutorMock;
import org.apache.flink.table.utils.PlannerMock;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link StreamTableEnvironmentImpl}.
 */
public class StreamTableEnvironmentImplTest {
	@Test
	public void testAppendStreamDoesNotOverwriteTableConfig() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> elements = env.fromElements(1, 2, 3);

		StreamTableEnvironmentImpl tEnv = getStreamTableEnvironment(env, elements);

		Time minRetention = Time.minutes(1);
		Time maxRetention = Time.minutes(10);
		tEnv.getConfig().setIdleStateRetentionTime(minRetention, maxRetention);
		Table table = tEnv.fromDataStream(elements);
		tEnv.toAppendStream(table, Row.class);

		assertThat(
			tEnv.getConfig().getMinIdleStateRetentionTime(),
			equalTo(minRetention.toMilliseconds()));
		assertThat(
			tEnv.getConfig().getMaxIdleStateRetentionTime(),
			equalTo(maxRetention.toMilliseconds()));
	}

	@Test
	public void testRetractStreamDoesNotOverwriteTableConfig() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> elements = env.fromElements(1, 2, 3);

		StreamTableEnvironmentImpl tEnv = getStreamTableEnvironment(env, elements);

		Time minRetention = Time.minutes(1);
		Time maxRetention = Time.minutes(10);
		tEnv.getConfig().setIdleStateRetentionTime(minRetention, maxRetention);
		Table table = tEnv.fromDataStream(elements);
		tEnv.toRetractStream(table, Row.class);

		assertThat(
			tEnv.getConfig().getMinIdleStateRetentionTime(),
			equalTo(minRetention.toMilliseconds()));
		assertThat(
			tEnv.getConfig().getMaxIdleStateRetentionTime(),
			equalTo(maxRetention.toMilliseconds()));
	}

	private StreamTableEnvironmentImpl getStreamTableEnvironment(
			StreamExecutionEnvironment env,
			DataStreamSource<Integer> elements) {
		TableConfig config = new TableConfig();
		CatalogManager catalogManager = new CatalogManager("cat", new GenericInMemoryCatalog("cat", "db"));
		ModuleManager moduleManager = new ModuleManager();
		return new StreamTableEnvironmentImpl(
			catalogManager,
			moduleManager,
			new FunctionCatalog(config, catalogManager, moduleManager),
			config,
			env,
			new TestPlanner(elements.getTransformation()),
			new ExecutorMock(),
			true
		);
	}

	private static class TestPlanner extends PlannerMock {
		private final Transformation<?> transformation;

		private TestPlanner(Transformation<?> transformation) {
			this.transformation = transformation;
		}

		@Override
		public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
			return Collections.singletonList(transformation);
		}
	}
}
