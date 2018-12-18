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

package org.apache.flink.test.state.operator.restore.keyed;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.test.state.operator.restore.ExecutionMode;

/**
 * Test state restoration for a keyed operator restore tests.
 */
public class KeyedComplexChainTest extends AbstractKeyedOperatorRestoreTestBase {

	public KeyedComplexChainTest(MigrationVersion migrationVersion) {
		super(migrationVersion);
	}

	@Override
	protected void createRestoredJob(StreamExecutionEnvironment env) {
		/**
		 * Source -> keyBy -> C(Window -> StatefulMap2) -> StatefulMap1
		 */
		SingleOutputStreamOperator<Tuple2<Integer, Integer>> source = KeyedJob.createIntegerTupleSource(env, ExecutionMode.RESTORE);

		SingleOutputStreamOperator<Integer> window = KeyedJob.createWindowFunction(ExecutionMode.RESTORE, source);

		SingleOutputStreamOperator<Integer> second = KeyedJob.createSecondStatefulMap(ExecutionMode.RESTORE, window);

		SingleOutputStreamOperator<Integer> first = KeyedJob.createFirstStatefulMap(ExecutionMode.RESTORE, second);
		first.startNewChain();
	}
}
