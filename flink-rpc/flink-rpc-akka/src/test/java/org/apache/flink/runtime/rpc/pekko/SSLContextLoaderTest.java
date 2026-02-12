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

package org.apache.flink.runtime.rpc.pekko;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SSLContextLoaderTest {

    @Test
    public void testCreaseSSLContextLoaderWithUnexistedCertificates() throws Exception {
        final Config pekkoSecurityConfig = pekkoConfig("");
        String sslTrustStore = pekkoSecurityConfig.getString("trust-store");

        assertThatThrownBy(
                        () ->
                                new SSLContextLoader(
                                        sslTrustStore, "sslProtocol", pekkoSecurityConfig))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Cannot load SSL context");
    }

    @Test
    public void testCreaseSSLContextLoaderWithWrongPekkoConfig() throws Exception {
        final Config pekkoSecurityConfig = pekkoConfig("wrong");
        String sslTrustStore = pekkoSecurityConfig.getString("trust-store");

        assertThatThrownBy(
                        () ->
                                new SSLContextLoader(
                                        sslTrustStore, "sslProtocol", pekkoSecurityConfig))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "hardcoded value: No configuration setting found for key 'trust-store-password'");
    }

    private static Config pekkoConfig(String prefix) {
        return ConfigFactory.parseMap(
                Map.of(
                        "trust-store",
                        "non-trust-store",
                        prefix + "trust-store-password",
                        "ts-pwd-123",
                        prefix + "cert-fingerprints",
                        List.of("F1:INGER:PRINT:01", "F2:INGER:PRINT:02"),
                        prefix + "key-store-type",
                        "JKS",
                        prefix + "trust-store-type",
                        "JKS",
                        prefix + "key-store",
                        "/tmp/keystore.jks",
                        prefix + "key-store-password",
                        "ks-pwd-456",
                        prefix + "key-password",
                        "key-pwd-789",
                        prefix + "random-number-generator",
                        "SHA1PRNG"));
    }
}
