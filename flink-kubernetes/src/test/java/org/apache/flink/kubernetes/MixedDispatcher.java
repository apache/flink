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

import io.fabric8.kubernetes.client.server.mock.KubernetesCrudDispatcher;
import io.fabric8.mockwebserver.ServerRequest;
import io.fabric8.mockwebserver.ServerResponse;
import io.fabric8.mockwebserver.dsl.HttpMethod;
import io.fabric8.mockwebserver.internal.SimpleRequest;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.Map;
import java.util.Queue;

/** A dispatcher that support both mock response and CRUD response. */
public class MixedDispatcher extends KubernetesCrudDispatcher {
    private final Map<ServerRequest, Queue<ServerResponse>> responses;

    public MixedDispatcher(Map<ServerRequest, Queue<ServerResponse>> responses) {
        this.responses = responses;
    }

    @Override
    public MockResponse dispatch(RecordedRequest request) {
        HttpMethod method = HttpMethod.valueOf(request.getMethod());
        String path = request.getPath();
        SimpleRequest key = new SimpleRequest(method, path);
        SimpleRequest keyForAnyMethod = new SimpleRequest(path);
        if (responses.containsKey(key)) {
            Queue<ServerResponse> queue = responses.get(key);
            return handleResponse(queue.peek(), queue, request);
        } else if (responses.containsKey(keyForAnyMethod)) {
            Queue<ServerResponse> queue = responses.get(keyForAnyMethod);
            return handleResponse(queue.peek(), queue, request);
        }

        return super.dispatch(request);
    }

    private MockResponse handleResponse(
            ServerResponse response, Queue<ServerResponse> queue, RecordedRequest request) {
        if (response == null) {
            return new MockResponse().setResponseCode(404);
        } else if (!response.isRepeatable()) {
            queue.remove();
        }
        return response.toMockResponse(request);
    }
}
