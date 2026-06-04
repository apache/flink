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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.List;

/** Test proper handling of IPv6 address literals in URLs. */
@ExtendWith(TestLoggerExtension.class)
// The job uses a GlobalWindow with an end-of-stream trigger, which results in a blocking
// (non-pipelined) data exchange. The AdaptiveScheduler only supports pipelined data exchanges.
@Tag("org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler")
// Skip the whole class when the host has no usable IPv6 address for the cluster to bind to.
// Evaluating this condition triggers class initialization (and thus the address probing), but the
// MiniCluster is only started afterwards in beforeAll, so a disabled class never starts one.
@EnabledIf("hasBindableIpv6Address")
class IPv6HostnamesITCase {
    private static final Logger LOG = LoggerFactory.getLogger(IPv6HostnamesITCase.class);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static Configuration getConfiguration() {
        final Inet6Address ipv6Address = Ipv6AddressHolder.ADDRESS;
        Configuration config = new Configuration();
        if (ipv6Address != null) {
            final String addressString = ipv6Address.getHostAddress();
            LOG.info("Test will use IPv6 address {} for connection tests", addressString);
            config.set(JobManagerOptions.ADDRESS, addressString);
            config.set(TaskManagerOptions.HOST, addressString);
        }
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
        return config;
    }

    private static boolean hasBindableIpv6Address() {
        return Ipv6AddressHolder.ADDRESS != null;
    }

    @Test
    void testClusterWithIPv6host() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // get input data
        DataStream<String> text = env.fromData(WordCountData.TEXT.split("\n"));

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                    @Override
                                    public void flatMap(
                                            String value, Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (String token : value.toLowerCase().split("\\W+")) {
                                            if (token.length() > 0) {
                                                out.collect(new Tuple2<String, Integer>(token, 1));
                                            }
                                        }
                                    }
                                })
                        .keyBy(x -> x.f0)
                        .window(GlobalWindows.createWithEndOfStreamTrigger())
                        .reduce(
                                new ReduceFunction<Tuple2<String, Integer>>() {
                                    @Override
                                    public Tuple2<String, Integer> reduce(
                                            Tuple2<String, Integer> value1,
                                            Tuple2<String, Integer> value2)
                                            throws Exception {
                                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                                    }
                                });

        List<Tuple2<String, Integer>> result =
                CollectionUtil.iteratorToList(counts.executeAndCollect());

        TestBaseUtils.compareResultAsText(result, WordCountData.COUNTS_AS_TUPLES);
    }

    // Resolves a bindable non-loopback IPv6 address lazily and once. Holding it in a nested class
    // lets getConfiguration() and the @EnabledIf guard share a single result without depending on
    // the declaration order of the extension/config fields above.
    private static final class Ipv6AddressHolder {
        static final Inet6Address ADDRESS = getLocalIPv6Address();
    }

    private static Inet6Address getLocalIPv6Address() {
        try {
            Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface netInterface = e.nextElement();

                // for each address of the network interface
                Enumeration<InetAddress> ee = netInterface.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress addr = ee.nextElement();

                    if (addr instanceof Inet6Address
                            && (!addr.isLoopbackAddress())
                            && (!addr.isAnyLocalAddress())) {
                        // see if it is possible to bind to the address
                        InetSocketAddress socketAddress = new InetSocketAddress(addr, 0);

                        try {
                            LOG.info("Considering address {}", addr);

                            // test whether we can bind a socket to that address
                            LOG.info("Testing whether sockets can bind to {}", addr);
                            ServerSocket sock = new ServerSocket();
                            sock.bind(socketAddress);
                            sock.close();

                            // test whether Pekko's netty can bind to the address
                            LOG.info("Testing whether Pekko can use {}", addr);
                            final RpcService rpcService =
                                    RpcSystem.load()
                                            // this port is only used for advertising (==no port
                                            // conflicts) since we explicitly provide a bind port
                                            .remoteServiceBuilder(new Configuration(), null, "8081")
                                            .withBindAddress(addr.getHostAddress())
                                            .withBindPort(0)
                                            .createAndStart();
                            rpcService.closeAsync().get();

                            LOG.info("Using address {}", addr);
                            return (Inet6Address) addr;
                        } catch (IOException ignored) {
                            // fall through the loop
                        }
                    }
                }
            }

            return null;
        } catch (Exception e) {
            LOG.debug("No bindable non-loopback IPv6 address available", e);
            return null;
        }
    }
}
