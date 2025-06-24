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

package org.apache.flink.kubernetes;

import io.fabric8.kubernetes.client.Client;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.mockwebserver.Context;
import io.fabric8.mockwebserver.ServerRequest;
import io.fabric8.mockwebserver.ServerResponse;
import io.fabric8.mockwebserver.dsl.MockServerExpectation;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/** The mock server that host MixedDispatcher. */
public class MixedKubernetesServerExtension implements BeforeEachCallback, AfterEachCallback {

    private KubernetesMockServer mock;
    private final boolean https;

    private final boolean crudMode;

    private final MockWebServer mockWebServer;

    private final List<Client> clients;

    public MixedKubernetesServerExtension(boolean https, boolean crudMode) {
        this.https = https;
        this.crudMode = crudMode;
        mockWebServer = new MockWebServer();
        clients = new ArrayList<>();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        final HashMap<ServerRequest, Queue<ServerResponse>> response = new HashMap<>();
        mock =
                crudMode
                        ? new KubernetesMockServer(
                                new Context(),
                                mockWebServer,
                                response,
                                new MixedDispatcher(response),
                                true)
                        : new KubernetesMockServer(mockWebServer, response, https);
        mock.init();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        mock.destroy();
        clients.forEach(Client::close);
    }

    public NamespacedKubernetesClient createClient() {
        NamespacedKubernetesClient client = mock.createClient();
        clients.add(client);
        return client;
    }

    public RecordedRequest takeRequest(long timeout, TimeUnit unit) throws Exception {
        return mockWebServer.takeRequest(timeout, unit);
    }

    public int getRequestCount() {
        return mockWebServer.getRequestCount();
    }

    public MockServerExpectation expect() {
        return mock.expect();
    }
}
