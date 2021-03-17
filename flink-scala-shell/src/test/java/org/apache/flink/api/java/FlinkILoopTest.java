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

package org.apache.flink.api.java;

import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import scala.Option;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Integration tests for {@link FlinkILoop}. */
public class FlinkILoopTest extends TestLogger {

    @Test
    public void testConfigurationForwarding() {
        Configuration configuration = new Configuration();
        configuration.setString("foobar", "foobar");
        configuration.setString(JobManagerOptions.ADDRESS, "localhost");
        configuration.setInteger(JobManagerOptions.PORT, 6123);

        FlinkILoop flinkILoop = new FlinkILoop(configuration, Option.<String[]>empty());

        ExecutionEnvironment env = flinkILoop.scalaBenv().getJavaEnv();
        assertTrue(env instanceof ScalaShellEnvironment);

        Configuration forwardedConfiguration = env.getConfiguration();
        assertEquals(configuration, forwardedConfiguration);
    }

    @Test
    public void testConfigurationForwardingStreamEnvironment() {
        Configuration configuration = new Configuration();
        configuration.setString("foobar", "foobar");
        configuration.setString(JobManagerOptions.ADDRESS, "localhost");
        configuration.setInteger(JobManagerOptions.PORT, 6123);

        FlinkILoop flinkILoop = new FlinkILoop(configuration, Option.<String[]>empty());

        StreamExecutionEnvironment streamEnv = flinkILoop.scalaSenv().getJavaEnv();
        assertTrue(streamEnv instanceof ScalaShellStreamEnvironment);

        ScalaShellStreamEnvironment remoteStreamEnv = (ScalaShellStreamEnvironment) streamEnv;
        Configuration forwardedConfiguration = remoteStreamEnv.getClientConfiguration();
        assertEquals(configuration, forwardedConfiguration);
    }
}
