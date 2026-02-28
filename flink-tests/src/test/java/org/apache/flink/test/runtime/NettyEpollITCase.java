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

package org.apache.flink.test.runtime;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;

import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test network stack with taskmanager.network.netty.transport set to epoll. This test can only run
 * on linux. On other platforms it's basically a NO-OP. See
 * https://github.com/apache/flink-shaded/issues/30
 */
@ExtendWith(TestLoggerExtension.class)
class NettyEpollITCase {

    private static final int NUM_TASK_MANAGERS = 2;

    @Test
    void testNettyEpoll() throws Exception {
        Optional<MiniClusterWithClientResource> clusterOpt = trySetUpCluster();
        assumeThat(clusterOpt).as("This test is only supported on linux").isPresent();
        MiniClusterWithClientResource cluster = clusterOpt.get();

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(NUM_TASK_MANAGERS);

            DataStream<Integer> input = env.fromData(1, 2, 3, 4, 1, 2, 3, 42);
            input.keyBy(
                            new KeySelector<Integer, Integer>() {
                                @Override
                                public Integer getKey(Integer value) throws Exception {
                                    return value;
                                }
                            })
                    .sum(0)
                    .print();

            env.execute();
        } finally {
            cluster.after();
        }
    }

    private Optional<MiniClusterWithClientResource> trySetUpCluster() throws Exception {
        try {
            Configuration config = new Configuration();
            MiniClusterWithClientResource cluster =
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(config)
                                    .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                    .setNumberSlotsPerTaskManager(1)
                                    .build());
            cluster.before();
            return Optional.of(cluster);
        } catch (UnsatisfiedLinkError ex) {
            // If we failed to init netty because we are not on Linux platform, abort the test.
            if (findThrowableWithMessage(ex, "Only supported on Linux").isPresent()) {
                return Optional.empty();
            }
            throw ex;
        }
    }
}
