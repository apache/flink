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

package org.apache.flink.fs.s3native;

import org.apache.flink.fs.s3native.NativeS3FileSystemFactory.RetryMode;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a client built for either {@link RetryMode} retries what an object store answers with
 * SlowDown, against a local server that throttles a number of requests and then answers.
 */
class S3ClientProviderRetryModeTest {

    private static final String SLOW_DOWN =
            "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message>"
                    + "<RequestId>1</RequestId></Error>";

    private final AtomicInteger requests = new AtomicInteger();

    private HttpServer server;
    private int throttledRequests;
    private S3ClientProvider provider;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    if (requests.incrementAndGet() <= throttledRequests) {
                        respond(exchange, 503, SLOW_DOWN);
                    } else {
                        respond(exchange, 200, "hello");
                    }
                });
        server.start();
    }

    @AfterEach
    void stopServer() throws Exception {
        if (provider != null) {
            provider.close();
        }
        server.stop(0);
    }

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testAThrottledRequestIsRetriedUntilTheStoreAnswers(RetryMode mode) {
        throttledRequests = 1;
        provider = provider(mode, 5);

        String body = getObject().asUtf8String();

        assertThat(body).isEqualTo("hello");
        assertThat(requests).hasValue(2);
    }

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testARequestThatStaysThrottledFailsAfterTheConfiguredRetries(RetryMode mode) {
        throttledRequests = Integer.MAX_VALUE;
        provider = provider(mode, 1);

        assertThatThrownBy(this::getObject)
                .isInstanceOfSatisfying(
                        S3Exception.class, e -> assertThat(e.statusCode()).isEqualTo(503))
                .hasMessageContaining("SDK Attempt Count: 2");
        assertThat(requests).hasValue(2);
    }

    private ResponseBytes<GetObjectResponse> getObject() {
        return provider.getS3Client()
                .getObjectAsBytes(GetObjectRequest.builder().bucket("bucket").key("key").build());
    }

    private S3ClientProvider provider(RetryMode mode, int maxRetries) {
        return S3ClientProvider.builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .region("us-east-1")
                .pathStyleAccess(true)
                .accessKey("access")
                .secretKey("secret")
                .retryMode(mode)
                .maxRetries(maxRetries)
                .retryBaseDelay(Duration.ofMillis(1))
                .retryThrottleBaseDelay(Duration.ofMillis(1))
                .retryMaxBackoff(Duration.ofMillis(10))
                .build();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
