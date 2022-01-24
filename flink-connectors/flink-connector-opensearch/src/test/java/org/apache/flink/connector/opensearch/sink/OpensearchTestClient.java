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

package org.apache.flink.connector.opensearch.sink;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class OpensearchTestClient {
    private static final String DATA_FIELD_NAME = "data";
    private final RestHighLevelClient client;

    OpensearchTestClient(RestHighLevelClient client) {
        this.client = client;
    }

    GetResponse getResponse(String index, int id) throws IOException {
        return client.get(new GetRequest(index, Integer.toString(id)), RequestOptions.DEFAULT);
    }

    void assertThatIdsAreNotWritten(String index, int... ids) throws IOException {
        for (final int id : ids) {
            try {
                final GetResponse response = getResponse(index, id);
                assertFalse(
                        response.isExists(), String.format("Id %s is unexpectedly present.", id));
            } catch (OpenSearchStatusException e) {
                assertEquals(404, e.status().getStatus());
            }
        }
    }

    void assertThatIdsAreWritten(String index, int... ids)
            throws IOException, InterruptedException {
        for (final int id : ids) {
            GetResponse response;
            do {
                response = getResponse(index, id);
                Thread.sleep(10);
            } while (response.isSourceEmpty());
            assertEquals(buildMessage(id), response.getSource().get(DATA_FIELD_NAME));
        }
    }

    String getDataFieldName() {
        return DATA_FIELD_NAME;
    }

    static String buildMessage(int id) {
        return "test-" + id;
    }
}
