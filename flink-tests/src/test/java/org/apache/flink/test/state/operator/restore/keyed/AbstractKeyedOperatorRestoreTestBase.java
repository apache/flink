/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.state.operator.restore.keyed;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.test.state.operator.restore.AbstractOperatorRestoreTestBase;
import org.apache.flink.test.state.operator.restore.ExecutionMode;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Base class for all keyed operator restore tests.
 */
@RunWith(Parameterized.class)
public abstract class AbstractKeyedOperatorRestoreTestBase extends AbstractOperatorRestoreTestBase {

	private final MigrationVersion migrationVersion;

	@Parameterized.Parameters(name = "Migrate Savepoint: {0}")
	public static Collection<MigrationVersion> parameters () {
		return Arrays.asList(
			MigrationVersion.v1_2,
			MigrationVersion.v1_3,
			MigrationVersion.v1_4,
			MigrationVersion.v1_5,
			MigrationVersion.v1_6);
	}

	public AbstractKeyedOperatorRestoreTestBase(MigrationVersion migrationVersion) {
		this.migrationVersion = migrationVersion;
	}

	@Override
	public void createMigrationJob(StreamExecutionEnvironment env) {
		/**
		 * Source -> keyBy -> C(Window -> StatefulMap1 -> StatefulMap2)
		 */
		SingleOutputStreamOperator<Tuple2<Integer, Integer>> source = KeyedJob.createIntegerTupleSource(env, ExecutionMode.MIGRATE);

		SingleOutputStreamOperator<Integer> window = KeyedJob.createWindowFunction(ExecutionMode.MIGRATE, source);

		SingleOutputStreamOperator<Integer> first = KeyedJob.createFirstStatefulMap(ExecutionMode.MIGRATE, window);

		SingleOutputStreamOperator<Integer> second = KeyedJob.createSecondStatefulMap(ExecutionMode.MIGRATE, first);
	}

	@Override
	protected String getMigrationSavepointName() {
		return "complexKeyed-flink" + migrationVersion;
	}
}
