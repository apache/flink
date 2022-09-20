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

package org.apache.flink.connector.pulsar.table.testutils;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDataNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** A class to verify Pulsar authentication auth params map is created properly. */
public class MockPulsarAuthentication implements Authentication {
    public static String key1 = "key1";
    public static String key2 = "key2";
    public static String value1 = "value1";
    public static String value2 = "value2";

    @Override
    public String getAuthMethodName() {
        return "custom authentication";
    }

    @Override
    public AuthenticationDataProvider getAuthData() {
        return new AuthenticationDataNull();
    }

    @Override
    public AuthenticationDataProvider getAuthData(String brokerHostName) {
        return new AuthenticationDataNull();
    }

    @Override
    public void authenticationStage(
            String requestUrl,
            AuthenticationDataProvider authData,
            Map<String, String> previousResHeaders,
            CompletableFuture<Map<String, String>> authFuture) {
        Authentication.super.authenticationStage(
                requestUrl, authData, previousResHeaders, authFuture);
    }

    @Override
    public Set<Map.Entry<String, String>> newRequestHeader(
            String hostName,
            AuthenticationDataProvider authData,
            Map<String, String> previousResHeaders) {
        return new HashSet<>();
    }

    @Override
    public void configure(Map<String, String> authParams) {
        assert Objects.equals(authParams.get(key1), value1);
        assert Objects.equals(authParams.get(key2), value2);
    }

    @Override
    public void start() throws PulsarClientException {}

    @Override
    public void close() throws IOException {}
}
