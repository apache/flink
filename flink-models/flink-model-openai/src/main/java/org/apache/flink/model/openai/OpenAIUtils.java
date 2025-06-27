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

package org.apache.flink.model.openai;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.openai.client.OpenAIClientAsync;
import com.openai.client.okhttp.OpenAIOkHttpClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Utility class related to Open AI SDK. */
public class OpenAIUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OpenAIUtils.class);

    private static final Object LOCK = new Object();

    private static final Map<ReferenceKey, ReferenceValue> cache = new HashMap<>();

    public static OpenAIClientAsync createAsyncClient(String baseUrl, String apiKey, int numRetry) {
        synchronized (LOCK) {
            ReferenceKey key = new ReferenceKey(baseUrl, apiKey);
            ReferenceValue value = cache.get(key);
            if (value != null) {
                LOG.debug("Returning an existing OpenAI client.");
                value.referenceCount.incrementAndGet();
                return value.client;
            }

            LOG.debug("Building a new OpenAI client.");
            OpenAIClientAsync client =
                    OpenAIOkHttpClientAsync.builder()
                            .apiKey(apiKey)
                            .baseUrl(baseUrl)
                            .maxRetries(numRetry)
                            .build();
            cache.put(key, new ReferenceValue(client));
            return client;
        }
    }

    public static void releaseAsyncClient(String baseUrl, String apiKey) {
        synchronized (LOCK) {
            ReferenceKey key = new ReferenceKey(baseUrl, apiKey);
            ReferenceValue value = cache.get(key);
            Preconditions.checkNotNull(
                    value, "The creation and release of OpenAI client does not match.");
            int count = value.referenceCount.decrementAndGet();
            if (count == 0) {
                LOG.debug("Closing the OpenAI client.");
                cache.remove(key);
                value.client.close();
            }
        }
    }

    private static class ReferenceValue {
        private final OpenAIClientAsync client;
        private final AtomicInteger referenceCount;

        private ReferenceValue(OpenAIClientAsync client) {
            this.client = client;
            this.referenceCount = new AtomicInteger(1);
        }
    }

    private static class ReferenceKey {
        private final String baseUrl;
        private final String apiKey;

        private ReferenceKey(String baseUrl, String apiKey) {
            this.baseUrl = baseUrl;
            this.apiKey = apiKey;
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseUrl, apiKey);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ReferenceKey
                    && Objects.equals(baseUrl, ((ReferenceKey) obj).baseUrl)
                    && Objects.equals(apiKey, ((ReferenceKey) obj).apiKey);
        }
    }

    @VisibleForTesting
    static Map<ReferenceKey, ReferenceValue> getCache() {
        return cache;
    }
}
