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

package org.apache.flink.model.deepseek;

import org.apache.flink.util.Preconditions;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/** Utility class for managing DeepSeek clients. */
public class DeepSeekUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DeepSeekUtils.class);

    private static final Object LOCK = new Object();

    private static ReferenceValue client;

    public static OkHttpClient createClient(String baseUrl, String apiKey) {
        synchronized (LOCK) {
            if (client != null) {
                LOG.debug("Returning an existing DeepSeek client.");
                client.referenceCount.incrementAndGet();
                return client.client;
            }
            return new OkHttpClient.Builder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .readTimeout(Duration.ofSeconds(30))
                    .writeTimeout(Duration.ofSeconds(30))
                    .addInterceptor(chain -> {
                        okhttp3.Request original = chain.request();
                        okhttp3.Request request = original.newBuilder()
                                .header("Authorization", "Bearer " + apiKey)
                                .header("Content-Type", "application/json")
                                .method(original.method(), original.body())
                                .build();
                        return chain.proceed(request);
                    })
                    .build();
        }
    }



    /**
     * Releases a DeepSeek client instance. If the reference count reaches zero, the client is closed
     * and removed from the cache.
     *
     */
    public static void releaseClient() {
        synchronized (LOCK) {
            Preconditions.checkNotNull(
                    client, "The creation and release of DeepSeek client does not match.");
            int count = client.referenceCount.decrementAndGet();
            if (count == 0) {
                LOG.debug("Closing the DeepSeek client.");
                client = null;
            }
        }
    }

    private OkHttpClient createNewClient(String baseUrl, String apiKey) {
        return new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(30))
                .readTimeout(Duration.ofSeconds(30))
                .writeTimeout(Duration.ofSeconds(30))
                .addInterceptor(chain -> {
                    okhttp3.Request original = chain.request();
                    okhttp3.Request request = original.newBuilder()
                            .header("Authorization", "Bearer " + apiKey)
                            .header("Content-Type", "application/json")
                            .method(original.method(), original.body())
                            .build();
                    return chain.proceed(request);
                })
                .build();
    }

    /** Value stored in the cache, containing the client and reference count. */
    private static class ReferenceValue {
        private final OkHttpClient client;
        private final AtomicInteger referenceCount;

        private ReferenceValue(OkHttpClient client) {
            this.client = client;
            this.referenceCount = new AtomicInteger(0);
        }
    }
}
