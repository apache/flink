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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.Optional;

/**
 * Custom netty outbound handler factory in order to make custom changes on netty outbound data.
 * Good example usage of this API is custom authentication. When the user wants to send
 * authentication information then the instantiated channel handler can modify the HTTP request.
 * Since implementations are loaded with service loader it's discouraged to store any internal state
 * in factories.
 */
@Experimental
public interface OutboundChannelHandlerFactory {
    /**
     * Gives back priority of the {@link ChannelHandler}. The bigger the value is, the earlier it is
     * executed. If multiple handlers have the same priority then the order is not defined.
     *
     * @return the priority of the {@link ChannelHandler}.
     */
    int priority();

    /**
     * Creates new instance of {@link ChannelHandler}
     *
     * @param configuration The Flink {@link Configuration}.
     * @return {@link ChannelHandler} or null if no custom handler needs to be created.
     * @throws ConfigurationException Thrown, if the handler configuration is incorrect.
     */
    Optional<ChannelHandler> createHandler(Configuration configuration)
            throws ConfigurationException;
}
