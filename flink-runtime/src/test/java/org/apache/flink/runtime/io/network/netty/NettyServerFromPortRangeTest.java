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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.PortRange;
import org.apache.flink.util.NetUtils;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for initializing Netty server from a port range. */
class NettyServerFromPortRangeTest {

    @Test
    void testStartMultipleNettyServerSameConfig() throws Exception {
        NetUtils.Port port1 = NetUtils.getAvailablePort();
        NetUtils.Port port2;
        do {
            port2 = NetUtils.getAvailablePort();
        } while (port1.getPort() == port2.getPort());

        NettyConfig config = getConfig(port1, port2);
        NettyServer nettyServer1 = new NettyServer(config);
        int listeningPort1 =
                nettyServer1.init(new NettyTestUtil.NoOpProtocol(), new NettyBufferPool(1));

        NettyServer nettyServer2 = new NettyServer(config);
        int listeningPort2 =
                nettyServer2.init(new NettyTestUtil.NoOpProtocol(), new NettyBufferPool(1));

        assertThat(listeningPort1).isEqualTo(port1.getPort());
        assertThat(listeningPort2).isEqualTo(port2.getPort());
    }

    private NettyConfig getConfig(NetUtils.Port... ports) {
        String portRangeStr =
                Arrays.stream(ports)
                        .map(NetUtils.Port::getPort)
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));

        return new NettyConfig(
                InetAddress.getLoopbackAddress(),
                new PortRange(portRangeStr),
                NettyTestUtil.DEFAULT_SEGMENT_SIZE,
                1,
                new Configuration());
    }
}
