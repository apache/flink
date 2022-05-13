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

package org.apache.flink.connector.elasticsearch.sink;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

abstract class TestClientBase {

    static final String DOCUMENT_TYPE = "test-document-type";
    private static final String DATA_FIELD_NAME = "data";
    final RestHighLevelClient client;

    TestClientBase(RestHighLevelClient client) {
        this.client = client;
    }

    abstract GetResponse getResponse(String index, int id) throws IOException;

    void assertThatIdsAreNotWritten(String index, int... ids) throws IOException {
        for (final int id : ids) {
            try {
                final GetResponse response = getResponse(index, id);
                assertFalse(
                        response.isExists(), String.format("Id %s is unexpectedly present.", id));
            } catch (ElasticsearchStatusException e) {
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
