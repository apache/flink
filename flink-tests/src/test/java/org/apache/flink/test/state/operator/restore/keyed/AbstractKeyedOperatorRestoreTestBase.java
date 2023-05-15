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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.state.operator.restore.AbstractOperatorRestoreTestBase;
import org.apache.flink.test.state.operator.restore.ExecutionMode;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Base class for all keyed operator restore tests. */
@RunWith(Parameterized.class)
public abstract class AbstractKeyedOperatorRestoreTestBase extends AbstractOperatorRestoreTestBase {

    public AbstractKeyedOperatorRestoreTestBase(FlinkVersion flinkVersion) {
        super(flinkVersion);
    }

    @Override
    public void createMigrationJob(StreamExecutionEnvironment env) {
        /** Source -> keyBy -> C(Window -> StatefulMap1 -> StatefulMap2) */
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> source =
                KeyedJob.createIntegerTupleSource(env, ExecutionMode.MIGRATE);

        SingleOutputStreamOperator<Integer> window =
                KeyedJob.createWindowFunction(ExecutionMode.MIGRATE, source);

        SingleOutputStreamOperator<Integer> first =
                KeyedJob.createFirstStatefulMap(ExecutionMode.MIGRATE, window);

        SingleOutputStreamOperator<Integer> second =
                KeyedJob.createSecondStatefulMap(ExecutionMode.MIGRATE, first);
    }

    @Override
    protected String getMigrationSavepointName(FlinkVersion flinkVersion) {
        return "complexKeyed-flink" + flinkVersion;
    }
}
