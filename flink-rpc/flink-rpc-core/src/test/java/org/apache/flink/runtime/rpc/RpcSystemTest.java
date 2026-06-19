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

package org.apache.flink.runtime.rpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

class RpcSystemTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            RpcSystemLoader.class,
                            // usually this loader would be loaded first because it appears first
                            LowPriorityRpcSystemLoader.class.getName(),
                            HighPriorityRpcSystemLoader.class.getName())
                    .build();

    @Test
    void testRpcSystemPrioritization() {
        assertThat(RpcSystem.load())
                .as("The wrong loader was selected.")
                .isInstanceOf(DummyTestRpcSystem.class);
    }

    /** Test {@link RpcSystemLoader} with a high priority. */
    public static final class HighPriorityRpcSystemLoader implements RpcSystemLoader {

        @Override
        public int getLoadPriority() {
            return 0;
        }

        @Override
        public RpcSystem loadRpcSystem(Configuration config) {
            return new DummyTestRpcSystem();
        }
    }

    /** Test {@link RpcSystemLoader} with a low priority. */
    public static final class LowPriorityRpcSystemLoader implements RpcSystemLoader {

        @Override
        public int getLoadPriority() {
            return 4;
        }

        @Override
        public RpcSystem loadRpcSystem(Configuration config) {
            return null;
        }
    }

    private static class DummyTestRpcSystem implements RpcSystem {

        @Override
        public RpcServiceBuilder localServiceBuilder(Configuration configuration) {
            return null;
        }

        @Override
        public RpcServiceBuilder remoteServiceBuilder(
                Configuration configuration,
                @Nullable String externalAddress,
                String externalPortRange) {
            return null;
        }

        @Override
        public String getRpcUrl(
                String hostname,
                int port,
                String endpointName,
                AddressResolution addressResolution,
                Configuration config) {
            return null;
        }

        @Override
        public InetSocketAddress getInetSocketAddressFromRpcUrl(String url) {
            return null;
        }

        @Override
        public long getMaximumMessageSizeInBytes(Configuration config) {
            return 0;
        }
    }
}
