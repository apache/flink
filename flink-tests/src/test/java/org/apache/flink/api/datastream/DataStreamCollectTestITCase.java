/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@code DataStream} collect methods.
 *
 * <p><b>Important:</b> This test does not use a shared {@code MiniCluster} to validate collection
 * on bounded streams after the Flink session has completed.
 */
public class DataStreamCollectTestITCase extends TestLogger {

    @Test
    public void testStreamingCollect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Integer> stream = env.fromElements(1, 2, 3);

        try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
            List<Integer> results = CollectionUtil.iteratorToList(iterator);
            Assert.assertThat(
                    "Failed to collect all data from the stream",
                    results,
                    Matchers.containsInAnyOrder(1, 2, 3));
        }
    }

    @Test
    public void testStreamingCollectAndLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Integer> stream = env.fromElements(1, 2, 3, 4, 5);

        List<Integer> results = stream.executeAndCollect(1);
        Assert.assertEquals(
                "Failed to collect the correct number of elements from the stream",
                1,
                results.size());
    }

    @Test
    public void testBoundedCollect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Integer> stream = env.fromElements(1, 2, 3);

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, DataStreamCollectTestITCase.class.getClassLoader());

        try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
            List<Integer> results = CollectionUtil.iteratorToList(iterator);
            Assert.assertThat(
                    "Failed to collect all data from the stream",
                    results,
                    Matchers.containsInAnyOrder(1, 2, 3));
        }
    }

    @Test
    public void testBoundedCollectAndLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, DataStreamCollectTestITCase.class.getClassLoader());

        DataStream<Integer> stream = env.fromElements(1, 2, 3, 4, 5);

        List<Integer> results = stream.executeAndCollect(1);
        Assert.assertEquals(
                "Failed to collect the correct number of elements from the stream",
                1,
                results.size());
    }
}
