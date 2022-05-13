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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.elasticsearch.sink.TestClientBase.DOCUMENT_TYPE;

class TestEmitter implements ElasticsearchEmitter<Tuple2<Integer, String>> {

    private final String index;
    private final XContentBuilderProvider xContentBuilderProvider;
    private final String dataFieldName;

    public static TestEmitter jsonEmitter(String index, String dataFieldName) {
        return new TestEmitter(index, dataFieldName, XContentFactory::jsonBuilder);
    }

    public static TestEmitter smileEmitter(String index, String dataFieldName) {
        return new TestEmitter(index, dataFieldName, XContentFactory::smileBuilder);
    }

    private TestEmitter(
            String index, String dataFieldName, XContentBuilderProvider xContentBuilderProvider) {
        this.dataFieldName = dataFieldName;
        this.index = index;
        this.xContentBuilderProvider = xContentBuilderProvider;
    }

    @Override
    public void emit(
            Tuple2<Integer, String> element, SinkWriter.Context context, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }

    private IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
        Map<String, Object> document = new HashMap<>();
        document.put(dataFieldName, element.f1);
        try {
            return new IndexRequest(index)
                    .id(element.f0.toString())
                    .type(DOCUMENT_TYPE)
                    .source(xContentBuilderProvider.getBuilder().map(document));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface XContentBuilderProvider extends Serializable {
        XContentBuilder getBuilder() throws IOException;
    }
}
