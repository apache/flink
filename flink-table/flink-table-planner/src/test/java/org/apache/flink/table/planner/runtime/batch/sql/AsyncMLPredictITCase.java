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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMLPredictTableFunction;
import org.apache.flink.table.planner.runtime.utils.MLPredictITCaseBase;

import org.junit.jupiter.api.BeforeEach;

/** ITCase for async ML_PREDICT in batch mode. Tests {@link BatchExecMLPredictTableFunction}. */
public class AsyncMLPredictITCase extends MLPredictITCaseBase {

    private StreamExecutionEnvironment env;

    @BeforeEach
    @Override
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        super.before();
    }

    @Override
    protected TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        return StreamTableEnvironment.create(env, settings);
    }

    @Override
    protected boolean isAsync() {
        return true;
    }
}
