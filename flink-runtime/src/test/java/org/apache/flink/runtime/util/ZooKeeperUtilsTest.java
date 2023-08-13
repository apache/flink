/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ZooKeeperUtils}. */
class ZooKeeperUtilsTest {

    @Test
    void testZookeeperPathGeneration() {
        runZookeeperPathGenerationTest("/root/namespace", "root", "namespace");
        runZookeeperPathGenerationTest("/root/namespace", "/root/", "/namespace/");
        runZookeeperPathGenerationTest("/root/namespace", "//root//", "//namespace//");
        runZookeeperPathGenerationTest("/namespace", "////", "namespace");
        runZookeeperPathGenerationTest("/a/b", "//a//", "/b/");
        runZookeeperPathGenerationTest("/", "", "");
        runZookeeperPathGenerationTest("/root", "root", "////");
        runZookeeperPathGenerationTest("/", "");
        runZookeeperPathGenerationTest("/a/b/c/d", "a", "b", "c", "d");
    }

    @Test
    void testZooKeeperEnsembleConnectStringConfiguration() throws Exception {
        // ZooKeeper does not like whitespace in the quorum connect String.
        String actual, expected;
        Configuration conf = new Configuration();

        {
            expected = "localhost:2891";

            setQuorum(conf, expected);
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);

            setQuorum(conf, " localhost:2891 "); // with leading and trailing whitespace
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);

            setQuorum(conf, "localhost :2891"); // whitespace after port
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);
        }

        {
            expected = "localhost:2891,localhost:2891";

            setQuorum(conf, "localhost:2891,localhost:2891");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);

            setQuorum(conf, "localhost:2891, localhost:2891");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);

            setQuorum(conf, "localhost :2891, localhost:2891");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);

            setQuorum(conf, " localhost:2891, localhost:2891 ");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void testStartCuratorFrameworkFailed() throws Exception {
        TestingFatalErrorHandler handler = new TestingFatalErrorHandler();
        String errorMsg = "unexpected exception";
        final CuratorFrameworkFactory.Builder curatorFrameworkBuilder =
                CuratorFrameworkFactory.builder()
                        .connectString("localhost:2181")
                        .retryPolicy(new ExponentialBackoffRetry(1, 1))
                        .zookeeperFactory(
                                (s, i, watcher, b) -> {
                                    throw new RuntimeException(errorMsg);
                                })
                        .namespace("flink");
        ZooKeeperUtils.startCuratorFramework(curatorFrameworkBuilder, handler);
        assertThat(handler.getErrorFuture().get()).hasMessage(errorMsg);
    }

    private void runZookeeperPathGenerationTest(String expectedValue, String... paths) {
        final String result = ZooKeeperUtils.generateZookeeperPath(paths);

        assertThat(result).isEqualTo(expectedValue);
    }

    private Configuration setQuorum(Configuration conf, String quorum) {
        conf.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, quorum);
        return conf;
    }
}
