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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Elasticsearch data reader. */
public class ElasticsearchDataReader
        implements ExternalSystemDataReader<KeyValue<Integer, String>> {
    private final ElasticsearchClient client;
    private final String indexName;
    private final int pageLength;

    public ElasticsearchDataReader(ElasticsearchClient client, String indexName, int pageLength) {
        this.client = checkNotNull(client);
        this.indexName = checkNotNull(indexName);
        this.pageLength = pageLength;
    }

    @Override
    public List<KeyValue<Integer, String>> poll(Duration timeout) {
        client.refreshIndex(indexName);
        QueryParams params =
                QueryParams.newBuilder(indexName)
                        .sortField("key")
                        .pageLength(pageLength)
                        .trackTotalHits(true)
                        .build();
        return client.fetchAll(params);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
