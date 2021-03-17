/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test cases for MLEnvironment. */
public class MLEnvironmentTest {
    @Test
    public void testDefaultConstructor() {
        MLEnvironment mlEnvironment = new MLEnvironment();
        Assert.assertNotNull(mlEnvironment.getExecutionEnvironment());
        Assert.assertNotNull(mlEnvironment.getBatchTableEnvironment());
        Assert.assertNotNull(mlEnvironment.getStreamExecutionEnvironment());
        Assert.assertNotNull(mlEnvironment.getStreamTableEnvironment());
    }

    @Test
    public void testConstructWithBatchEnv() {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment =
                BatchTableEnvironment.create(executionEnvironment);

        MLEnvironment mlEnvironment =
                new MLEnvironment(executionEnvironment, batchTableEnvironment);

        Assert.assertSame(mlEnvironment.getExecutionEnvironment(), executionEnvironment);
        Assert.assertSame(mlEnvironment.getBatchTableEnvironment(), batchTableEnvironment);
    }

    @Test
    public void testConstructWithStreamEnv() {
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(
                        streamExecutionEnvironment,
                        EnvironmentSettings.newInstance().useOldPlanner().build());

        MLEnvironment mlEnvironment =
                new MLEnvironment(streamExecutionEnvironment, streamTableEnvironment);

        Assert.assertSame(
                mlEnvironment.getStreamExecutionEnvironment(), streamExecutionEnvironment);
        Assert.assertSame(mlEnvironment.getStreamTableEnvironment(), streamTableEnvironment);
    }

    @Test
    public void testRemoveDefaultMLEnvironment() {
        MLEnvironment defaultEnv = MLEnvironmentFactory.getDefault();
        MLEnvironmentFactory.remove(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
        assertEquals(
                "The default MLEnvironment should not have been removed",
                defaultEnv,
                MLEnvironmentFactory.get(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID));
    }
}
