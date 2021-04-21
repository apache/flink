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
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class NettyServerMultiPortsTest {

    @Test
    public void testStartMultipleNettyServerSameConfig() throws Exception {
        int firstPort = NetUtils.getAvailablePort();
        int secondPort = firstPort;
        while (secondPort == firstPort) {
            secondPort = NetUtils.getAvailablePort();
        }
        NettyConfig config = createNettyConfig(firstPort, secondPort);
        NettyServer nettyServer1 = new NettyServer(config);
        int firstBindPort = nettyServer1.init(new NoOpProtocol(), new NettyBufferPool(1));

        NettyServer nettyServer2 = new NettyServer(config);
        int secondBindPort = nettyServer2.init(new NoOpProtocol(), new NettyBufferPool(1));

        assertEquals(firstPort, firstBindPort);
        assertEquals(secondPort, secondBindPort);
    }

    private NettyConfig createNettyConfig(int firstPort, int secondPort) {
        String portRange = String.format("%s,%s", firstPort, secondPort);
        Iterator<Integer> portRangeIterator = NetUtils.getPortRangeFromString(portRange);
        return new NettyConfig(
                InetAddress.getLoopbackAddress(),
                portRangeIterator,
                portRange,
                NettyTestUtil.DEFAULT_SEGMENT_SIZE,
                1,
                new Configuration());
    }

    private static class NoOpProtocol extends NettyProtocol {
        NoOpProtocol() {
            super(null, null);
        }

        @Override
        public ChannelHandler[] getServerChannelHandlers() {
            return new ChannelHandler[0];
        }

        @Override
        public ChannelHandler[] getClientChannelHandlers() {
            return new ChannelHandler[0];
        }
    }
}
