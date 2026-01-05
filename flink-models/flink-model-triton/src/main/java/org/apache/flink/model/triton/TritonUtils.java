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

import org.apache.flink.annotation.VisibleForTesting;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for Triton Inference Server HTTP client management.
 *
 * <p>This class implements a reference-counted singleton pattern for OkHttpClient instances.
 * Multiple function instances sharing the same timeout and retry configuration will reuse the same
 * client, reducing resource consumption in high-parallelism scenarios.
 *
 * <p><b>Resource Management:</b>
 *
 * <ul>
 *   <li>Clients are cached by (timeout, maxRetries) key
 *   <li>Reference count tracks active users
 *   <li>Client is closed when reference count reaches zero
 *   <li>Thread-safe via synchronized blocks
 * </ul>
 *
 * <p><b>URL Construction:</b> The {@link #buildInferenceUrl} method normalizes endpoint URLs to
 * conform to Triton's REST API specification: {@code /v2/models/{name}/versions/{version}/infer}
 */
public class TritonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TritonUtils.class);

    private static final Object LOCK = new Object();

    private static final Map<ClientKey, ClientValue> cache = new HashMap<>();

    /**
     * Creates or retrieves a cached HTTP client with the specified configuration.
     *
     * <p>This method implements reference-counted client pooling. Clients with identical timeout
     * and retry settings are shared across multiple callers.
     *
     * @param timeoutMs Timeout in milliseconds for connect, read, and write operations
     * @param maxRetries Maximum retry attempts (note: OkHttp retries are automatic for connection
     *     failures only, not for HTTP errors)
     * @return A shared or new OkHttpClient instance
     */
    public static OkHttpClient createHttpClient(long timeoutMs, int maxRetries) {
        synchronized (LOCK) {
            ClientKey key = new ClientKey(timeoutMs, maxRetries);
            ClientValue value = cache.get(key);
            if (value != null) {
                LOG.debug("Returning an existing Triton HTTP client.");
                value.referenceCount.incrementAndGet();
                return value.client;
            }

            LOG.debug("Building a new Triton HTTP client.");
            OkHttpClient client =
                    new OkHttpClient.Builder()
                            .connectTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                            .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                            .writeTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                            .retryOnConnectionFailure(true)
                            .build();

            cache.put(key, new ClientValue(client));
            return client;
        }
    }

    /**
     * Releases a reference to an HTTP client. When the reference count reaches zero, the client is
     * closed and removed from the cache.
     *
     * @param client The client to release
     */
    public static void releaseHttpClient(OkHttpClient client) {
        synchronized (LOCK) {
            ClientKey keyToRemove = null;
            ClientValue valueToRemove = null;

            for (Map.Entry<ClientKey, ClientValue> entry : cache.entrySet()) {
                if (entry.getValue().client == client) {
                    keyToRemove = entry.getKey();
                    valueToRemove = entry.getValue();
                    break;
                }
            }

            if (valueToRemove != null) {
                int count = valueToRemove.referenceCount.decrementAndGet();
                if (count == 0) {
                    LOG.debug("Closing the Triton HTTP client.");
                    cache.remove(keyToRemove);
                    // OkHttpClient doesn't need explicit closing, but we can clean up resources
                    client.dispatcher().executorService().shutdown();
                    client.connectionPool().evictAll();
                }
            }
        }
    }

    /**
     * Builds the inference URL for a specific model and version.
     *
     * <p>This method normalizes various endpoint formats to the standard Triton REST API path:
     *
     * <pre>
     * Input: http://localhost:8000          → http://localhost:8000/v2/models/mymodel/versions/1/infer
     * Input: http://localhost:8000/v2       → http://localhost:8000/v2/models/mymodel/versions/1/infer
     * Input: http://localhost:8000/v2/models → http://localhost:8000/v2/models/mymodel/versions/1/infer
     * </pre>
     *
     * @param endpoint The base URL or partial URL of the Triton server
     * @param modelName The name of the model
     * @param modelVersion The version of the model (e.g., "1", "latest")
     * @return The complete inference endpoint URL
     */
    public static String buildInferenceUrl(String endpoint, String modelName, String modelVersion) {
        String baseUrl = endpoint.replaceAll("/*$", "");
        if (!baseUrl.endsWith("/v2/models")) {
            if (baseUrl.endsWith("/v2")) {
                baseUrl += "/models";
            } else {
                baseUrl += "/v2/models";
            }
        }
        return String.format("%s/%s/versions/%s/infer", baseUrl, modelName, modelVersion);
    }

    /** Builds the model metadata URL for a specific model and version. */
    public static String buildModelMetadataUrl(
            String endpoint, String modelName, String modelVersion) {
        String baseUrl = endpoint.replaceAll("/*$", "");
        if (!baseUrl.endsWith("/v2/models")) {
            if (baseUrl.endsWith("/v2")) {
                baseUrl += "/models";
            } else {
                baseUrl += "/v2/models";
            }
        }
        return String.format("%s/%s/versions/%s", baseUrl, modelName, modelVersion);
    }

    private static class ClientValue {
        private final OkHttpClient client;
        private final AtomicInteger referenceCount;

        private ClientValue(OkHttpClient client) {
            this.client = client;
            this.referenceCount = new AtomicInteger(1);
        }
    }

    private static class ClientKey {
        private final long timeoutMs;
        private final int maxRetries;

        private ClientKey(long timeoutMs, int maxRetries) {
            this.timeoutMs = timeoutMs;
            this.maxRetries = maxRetries;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timeoutMs, maxRetries);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ClientKey
                    && timeoutMs == ((ClientKey) obj).timeoutMs
                    && maxRetries == ((ClientKey) obj).maxRetries;
        }
    }

    @VisibleForTesting
    static Map<ClientKey, ClientValue> getCache() {
        return cache;
    }
}
