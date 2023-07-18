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

package org.apache.flink.client.testjar;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

/** Simulate a class setting a configuration. */
public class ForbidConfigurationJob {

    /** Configured savepoint path. */
    public static final String SAVEPOINT_PATH = "/flink/savepoints";

    public static void main(String[] args) throws Exception {
        final Configuration config = new Configuration();
        config.set(SavepointConfigOptions.SAVEPOINT_PATH, SAVEPOINT_PATH);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.fromCollection(Lists.newArrayList(1, 2, 3)).sinkTo(new DiscardingSink<>());
        env.execute();
    }
}
