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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.AfterEach;

/**
 * Base test class for ITCases of flink-table module in streaming mode. This class is an upgraded
 * version of {@link StreamingTestBase}, which migrates Junit4 to replace junit5. We recommend using
 * this class instead of {@link StreamingTestBase} for the batch ITCases of flink-table module in
 * the future.
 *
 * <p>The class will be renamed to StreamingTestBase after all batch ITCase are migrated to junit5.
 */
public class StreamingTestBaseV2 extends AbstractTestBaseV2 {

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    private boolean enableObjectReuse = true;

    @Override
    protected TableEnvironment getTableEnvironment() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        if (enableObjectReuse) {
            env.getConfig().enableObjectReuse();
        }
        tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
        return tEnv;
    }

    @AfterEach
    public void after() {
        StreamTestSink.clear();
        TestValuesTableFactory.clearAllData();
    }
}
