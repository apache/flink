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

package org.apache.flink.runtime.webmonitor.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.netty.Prio0InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio1InboundChannelHandlerFactory;
import org.apache.flink.runtime.rest.handler.router.Router;

import org.junit.Test;
import org.slf4j.Logger;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class WebFrontendBootstrapTest {
    @Test
    public void testHandlersMustBeLoaded() throws UnknownHostException, InterruptedException {
        WebFrontendBootstrap webUI =
                new WebFrontendBootstrap(
                        mock(Router.class),
                        mock(Logger.class),
                        null,
                        null,
                        null,
                        0,
                        new Configuration());
        assertEquals(webUI.inboundChannelHandlerFactories.size(), 2);
        assertTrue(
                webUI.inboundChannelHandlerFactories.get(0)
                        instanceof Prio1InboundChannelHandlerFactory);
        assertTrue(
                webUI.inboundChannelHandlerFactories.get(1)
                        instanceof Prio0InboundChannelHandlerFactory);
    }
}
