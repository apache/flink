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

package org.apache.flink.metrics.datadog;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.concurrent.TimeUnit;

/** Http client talking to Datadog. */
public class DatadogHttpClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatadogHttpClient.class);

    private static final String SERIES_URL_FORMAT =
            "https://app.datadoghq.%s/api/v1/series?api_key=%s";
    private static final String VALIDATE_URL_FORMAT =
            "https://app.datadoghq.%s/api/v1/validate?api_key=%s";
    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final int TIMEOUT = 3;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String seriesUrl;
    private final String validateUrl;
    private final OkHttpClient client;
    private final String apiKey;

    private final String proxyHost;
    private final int proxyPort;

    public DatadogHttpClient(
            String dgApiKey,
            String dgProxyHost,
            int dgProxyPort,
            DataCenter dataCenter,
            boolean validateApiKey) {
        if (dgApiKey == null || dgApiKey.isEmpty()) {
            throw new IllegalArgumentException("Invalid API key:" + dgApiKey);
        }
        apiKey = dgApiKey;
        proxyHost = dgProxyHost;
        proxyPort = dgProxyPort;

        Proxy proxy = getProxy();

        client =
                new OkHttpClient.Builder()
                        .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
                        .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
                        .readTimeout(TIMEOUT, TimeUnit.SECONDS)
                        .proxy(proxy)
                        .build();

        seriesUrl = String.format(SERIES_URL_FORMAT, dataCenter.getDomain(), apiKey);
        validateUrl = String.format(VALIDATE_URL_FORMAT, dataCenter.getDomain(), apiKey);

        if (validateApiKey) {
            validateApiKey();
        }
    }

    Proxy getProxy() {
        if (proxyHost == null) {
            return Proxy.NO_PROXY;
        } else {
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        }
    }

    private void validateApiKey() {
        Request r = new Request.Builder().url(validateUrl).get().build();

        try (Response response = client.newCall(r).execute()) {
            if (!response.isSuccessful()) {
                throw new IllegalArgumentException(String.format("API key: %s is invalid", apiKey));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed contacting Datadog to validate API key", e);
        }
    }

    public void send(DSeries request) throws Exception {
        String postBody = serialize(request);

        Request r =
                new Request.Builder()
                        .url(seriesUrl)
                        .post(RequestBody.create(MEDIA_TYPE, postBody))
                        .build();

        client.newCall(r).enqueue(EmptyCallback.getEmptyCallback());
    }

    public static String serialize(Object obj) throws JsonProcessingException {
        return MAPPER.writeValueAsString(obj);
    }

    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }

    /**
     * A handler for OkHttpClient callback. In case of error or failure it logs the error at warning
     * level.
     */
    protected static class EmptyCallback implements Callback {

        private static final EmptyCallback singleton = new EmptyCallback();

        public static Callback getEmptyCallback() {
            return singleton;
        }

        @Override
        public void onFailure(Call call, IOException e) {
            LOGGER.warn("Failed sending request to Datadog", e);
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            if (!response.isSuccessful()) {
                LOGGER.warn("Failed to send request to Datadog (response was {})", response);
            }

            response.close();
        }
    }
}
