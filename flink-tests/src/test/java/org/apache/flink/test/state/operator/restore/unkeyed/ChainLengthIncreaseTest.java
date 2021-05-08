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

package org.apache.flink.test.state.operator.restore.unkeyed;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.state.operator.restore.ExecutionMode;
import org.apache.flink.testutils.migration.MigrationVersion;

import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createFirstStatefulMap;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createSecondStatefulMap;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createSource;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createStatelessMap;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createThirdStatefulMap;

/**
 * Verifies that the state of all operator is restored if a topology change adds an operator to a
 * chain.
 */
public class ChainLengthIncreaseTest extends AbstractNonKeyedOperatorRestoreTestBase {

    public ChainLengthIncreaseTest(MigrationVersion migrationVersion) {
        super(migrationVersion);
    }

    @Override
    public void createRestoredJob(StreamExecutionEnvironment env) {
        /**
         * Original job: Source -> StatefulMap1 -> CHAIN(StatefulMap2 -> Map -> StatefulMap3)
         * Modified job: Source -> StatefulMap1 -> CHAIN(StatefulMap2 -> Map -> StatefulMap3 ->
         * StatefulMap4)
         */
        DataStream<Integer> source = createSource(env, ExecutionMode.RESTORE);

        SingleOutputStreamOperator<Integer> first =
                createFirstStatefulMap(ExecutionMode.RESTORE, source);
        first.startNewChain();

        SingleOutputStreamOperator<Integer> second =
                createSecondStatefulMap(ExecutionMode.RESTORE, first);
        second.startNewChain();

        SingleOutputStreamOperator<Integer> stateless = createStatelessMap(second);

        SingleOutputStreamOperator<Integer> stateless2 = createStatelessMap(stateless);

        SingleOutputStreamOperator<Integer> third =
                createThirdStatefulMap(ExecutionMode.RESTORE, stateless2);
    }
}
