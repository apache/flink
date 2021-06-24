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

package org.apache.flink.runtime.rpc;

import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.ServiceLoader;

/** Utils related to the {@link RpcSystem}. */
public final class RpcSystemUtils {

    /**
     * Convenient shortcut for constructing a remote RPC Service that takes care of checking for
     * null and empty optionals.
     *
     * @see RpcSystem#remoteServiceBuilder(Configuration, String, String)
     */
    public static RpcService createRemoteRpcService(
            RpcSystem rpcSystem,
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        RpcSystem.RpcServiceBuilder rpcServiceBuilder =
                rpcSystem.remoteServiceBuilder(configuration, externalAddress, externalPortRange);
        if (bindAddress != null) {
            rpcServiceBuilder = rpcServiceBuilder.withBindAddress(bindAddress);
        }
        if (bindPort.isPresent()) {
            rpcServiceBuilder = rpcServiceBuilder.withBindPort(bindPort.get());
        }
        return rpcServiceBuilder.createAndStart();
    }

    private RpcSystemUtils() {}

    static RpcSystem loadRpcSystem() {
        final ClassLoader classLoader = RpcUtils.class.getClassLoader();
        return ServiceLoader.load(RpcSystem.class, classLoader).iterator().next();
    }
}
