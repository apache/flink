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

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/** Utils that are dependent on the underlying RPC implementation. */
public interface RpcSystemUtils {

    /**
     * Constructs an RPC URL for the given parameters, that can be used to connect to the targeted
     * RpcService.
     *
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param config The configuration from which to deduce further settings.
     * @return The RPC URL of the specified RPC endpoint.
     */
    String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException;

    /**
     * Returns an {@link InetSocketAddress} corresponding to the given RPC url.
     *
     * @see #getRpcUrl
     * @param url RPC url
     * @return inet socket address
     * @throws Exception if the URL is invalid
     */
    InetSocketAddress getInetSocketAddressFromRpcUrl(String url) throws Exception;

    /**
     * Returns the maximum number of bytes that an RPC message may carry according to the given
     * configuration. If no limit exists then {@link Long#MAX_VALUE} should be returned.
     *
     * @param config Flink configuration
     * @return maximum number of bytes that an RPC message may carry
     */
    long getMaximumMessageSizeInBytes(Configuration config);
}
