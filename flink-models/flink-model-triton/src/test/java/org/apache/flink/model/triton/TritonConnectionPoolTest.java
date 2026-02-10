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

package org.apache.flink.model.triton;

import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TritonUtils} connection pool management. */
class TritonConnectionPoolTest {

    @AfterEach
    void tearDown() {
        // Clean up any remaining clients
        // (In production, clients are released via releaseHttpClient)
    }

    @Test
    void testClientCreationWithDefaultConfig() {
        long timeout = 10000;
        OkHttpClient client = TritonUtils.createHttpClient(timeout);

        assertThat(client).isNotNull();
        assertThat(client.readTimeoutMillis()).isEqualTo(timeout);
        assertThat(client.writeTimeoutMillis()).isEqualTo(timeout);

        TritonUtils.releaseHttpClient(client);
    }

    @Test
    void testClientCreationWithCustomConfig() {
        long timeout = 5000;
        TritonUtils.ConnectionPoolConfig config =
                new TritonUtils.ConnectionPoolConfig(
                        10, // maxIdle
                        60000, // keepAlive
                        50, // maxTotal
                        3000, // connectionTimeout
                        true, // reuse
                        false // monitoring
                        );

        OkHttpClient client = TritonUtils.createHttpClient(timeout, config);

        assertThat(client).isNotNull();
        assertThat(client.connectTimeoutMillis()).isEqualTo(3000);
        assertThat(client.readTimeoutMillis()).isEqualTo(timeout);
        assertThat(client.dispatcher().getMaxRequests()).isEqualTo(50);

        TritonUtils.releaseHttpClient(client);
    }

    @Test
    void testClientReuse() {
        long timeout = 10000;
        TritonUtils.ConnectionPoolConfig config =
                new TritonUtils.ConnectionPoolConfig(20, 300000, 100, 10000, true, false);

        // Create two clients with same configuration
        OkHttpClient client1 = TritonUtils.createHttpClient(timeout, config);
        OkHttpClient client2 = TritonUtils.createHttpClient(timeout, config);

        // Should be the same instance due to caching
        assertThat(client1).isSameAs(client2);

        TritonUtils.releaseHttpClient(client1);
        TritonUtils.releaseHttpClient(client2);
    }

    @Test
    void testClientNotReusedWithDifferentConfig() {
        long timeout = 10000;

        TritonUtils.ConnectionPoolConfig config1 =
                new TritonUtils.ConnectionPoolConfig(20, 300000, 100, 10000, true, false);

        TritonUtils.ConnectionPoolConfig config2 =
                new TritonUtils.ConnectionPoolConfig(30, 300000, 100, 10000, true, false);

        OkHttpClient client1 = TritonUtils.createHttpClient(timeout, config1);
        OkHttpClient client2 = TritonUtils.createHttpClient(timeout, config2);

        // Should be different instances due to different config
        assertThat(client1).isNotSameAs(client2);

        TritonUtils.releaseHttpClient(client1);
        TritonUtils.releaseHttpClient(client2);
    }

    @Test
    void testReferenceCounting() {
        long timeout = 10000;
        TritonUtils.ConnectionPoolConfig config =
                new TritonUtils.ConnectionPoolConfig(20, 300000, 100, 10000, true, false);

        // Create multiple references
        OkHttpClient client1 = TritonUtils.createHttpClient(timeout, config);
        OkHttpClient client2 = TritonUtils.createHttpClient(timeout, config);
        OkHttpClient client3 = TritonUtils.createHttpClient(timeout, config);

        assertThat(client1).isSameAs(client2).isSameAs(client3);

        // Release one reference - client should still be alive
        TritonUtils.releaseHttpClient(client1);

        // Get another reference - should still work
        OkHttpClient client4 = TritonUtils.createHttpClient(timeout, config);
        assertThat(client4).isSameAs(client1);

        // Release all remaining references
        TritonUtils.releaseHttpClient(client2);
        TritonUtils.releaseHttpClient(client3);
        TritonUtils.releaseHttpClient(client4);
    }

    @Test
    void testConnectionPoolWithReuseDisabled() {
        long timeout = 10000;
        TritonUtils.ConnectionPoolConfig config =
                new TritonUtils.ConnectionPoolConfig(
                        20,
                        300000,
                        100,
                        10000,
                        false, // reuse disabled
                        false);

        OkHttpClient client = TritonUtils.createHttpClient(timeout, config);

        // Connection pool should have 0 max idle connections when reuse is disabled
        assertThat(client.connectionPool().connectionCount()).isEqualTo(0);

        TritonUtils.releaseHttpClient(client);
    }

    @Test
    void testConnectionPoolWithReuseEnabled() {
        long timeout = 10000;
        TritonUtils.ConnectionPoolConfig config =
                new TritonUtils.ConnectionPoolConfig(
                        20,
                        300000,
                        100,
                        10000,
                        true, // reuse enabled
                        false);

        OkHttpClient client = TritonUtils.createHttpClient(timeout, config);

        // Connection pool should be configured
        assertThat(client.connectionPool()).isNotNull();

        TritonUtils.releaseHttpClient(client);
    }

    @Test
    void testDispatcherConfiguration() {
        long timeout = 10000;
        int maxTotal = 75;
        TritonUtils.ConnectionPoolConfig config =
                new TritonUtils.ConnectionPoolConfig(20, 300000, maxTotal, 10000, true, false);

        OkHttpClient client = TritonUtils.createHttpClient(timeout, config);

        assertThat(client.dispatcher().getMaxRequests()).isEqualTo(maxTotal);
        assertThat(client.dispatcher().getMaxRequestsPerHost()).isEqualTo(maxTotal);

        TritonUtils.releaseHttpClient(client);
    }

    @Test
    void testBuildInferenceUrl() {
        // Test various endpoint formats
        assertThat(TritonUtils.buildInferenceUrl("http://localhost:8000", "mymodel", "1"))
                .isEqualTo("http://localhost:8000/v2/models/mymodel/versions/1/infer");

        assertThat(TritonUtils.buildInferenceUrl("http://localhost:8000/", "mymodel", "1"))
                .isEqualTo("http://localhost:8000/v2/models/mymodel/versions/1/infer");

        assertThat(TritonUtils.buildInferenceUrl("http://localhost:8000/v2", "mymodel", "1"))
                .isEqualTo("http://localhost:8000/v2/models/mymodel/versions/1/infer");

        assertThat(
                        TritonUtils.buildInferenceUrl(
                                "http://localhost:8000/v2/models", "mymodel", "1"))
                .isEqualTo("http://localhost:8000/v2/models/mymodel/versions/1/infer");

        assertThat(TritonUtils.buildInferenceUrl("http://localhost:8000///", "mymodel", "latest"))
                .isEqualTo("http://localhost:8000/v2/models/mymodel/versions/latest/infer");
    }

    @Test
    void testConnectionPoolConfigEquality() {
        TritonUtils.ConnectionPoolConfig config1 =
                new TritonUtils.ConnectionPoolConfig(20, 300000, 100, 10000, true, false);

        TritonUtils.ConnectionPoolConfig config2 =
                new TritonUtils.ConnectionPoolConfig(20, 300000, 100, 10000, true, false);

        TritonUtils.ConnectionPoolConfig config3 =
                new TritonUtils.ConnectionPoolConfig(30, 300000, 100, 10000, true, false);

        assertThat(config1).isEqualTo(config2);
        assertThat(config1).isNotEqualTo(config3);
        assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
    }
}
