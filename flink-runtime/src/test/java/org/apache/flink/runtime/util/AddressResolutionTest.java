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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcSystem;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Unit tests for respecting {@link AddressResolution}. */
class AddressResolutionTest {

    private static final RpcSystem RPC_SYSTEM = RpcSystem.load();

    private static final String ENDPOINT_NAME = "endpoint";
    private static final String NON_EXISTING_HOSTNAME = "foo.bar.com.invalid";
    private static final int PORT = 17234;

    @BeforeAll
    static void check() {
        checkPreconditions();
    }

    private static void checkPreconditions() {
        // the test can only work if the invalid URL cannot be resolves
        // some internet providers resolve unresolvable URLs to navigational aid servers,
        // voiding this test.
        boolean throwsException;

        try {
            //noinspection ResultOfMethodCallIgnored
            InetAddress.getByName(NON_EXISTING_HOSTNAME);
            throwsException = false;
        } catch (UnknownHostException e) {
            throwsException = true;
        }

        assumeThat(throwsException).isTrue();
    }

    @Test
    void testNoAddressResolution() throws UnknownHostException {
        RPC_SYSTEM.getRpcUrl(
                NON_EXISTING_HOSTNAME,
                PORT,
                ENDPOINT_NAME,
                AddressResolution.NO_ADDRESS_RESOLUTION,
                new Configuration());
    }

    @Test
    void testTryAddressResolution() {
        assertThatThrownBy(
                        () ->
                                RPC_SYSTEM.getRpcUrl(
                                        NON_EXISTING_HOSTNAME,
                                        PORT,
                                        ENDPOINT_NAME,
                                        AddressResolution.TRY_ADDRESS_RESOLUTION,
                                        new Configuration()))
                .isInstanceOf(UnknownHostException.class);
    }
}
