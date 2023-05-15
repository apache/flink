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

import org.apache.flink.FlinkVersion;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.state.operator.restore.AbstractOperatorRestoreTestBase;
import org.apache.flink.test.state.operator.restore.ExecutionMode;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createFirstStatefulMap;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createSecondStatefulMap;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createSource;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createStatelessMap;
import static org.apache.flink.test.state.operator.restore.unkeyed.NonKeyedJob.createThirdStatefulMap;

/** Base class for all non-keyed operator restore tests. */
@RunWith(Parameterized.class)
public abstract class AbstractNonKeyedOperatorRestoreTestBase
        extends AbstractOperatorRestoreTestBase {

    protected AbstractNonKeyedOperatorRestoreTestBase(FlinkVersion flinkVersion) {
        super(flinkVersion);
    }

    @Override
    public void createMigrationJob(StreamExecutionEnvironment env) {
        /** Source -> StatefulMap1 -> CHAIN(StatefulMap2 -> Map -> StatefulMap3) */
        DataStream<Integer> source = createSource(env, ExecutionMode.MIGRATE);

        SingleOutputStreamOperator<Integer> first =
                createFirstStatefulMap(ExecutionMode.MIGRATE, source);
        first.startNewChain();

        SingleOutputStreamOperator<Integer> second =
                createSecondStatefulMap(ExecutionMode.MIGRATE, first);
        second.startNewChain();

        SingleOutputStreamOperator<Integer> stateless = createStatelessMap(second);

        SingleOutputStreamOperator<Integer> third =
                createThirdStatefulMap(ExecutionMode.MIGRATE, stateless);
    }

    @Override
    protected String getMigrationSavepointName(FlinkVersion flinkVersion) {
        return "nonKeyed-flink" + flinkVersion;
    }
}
