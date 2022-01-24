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

package org.apache.flink.streaming.connectors.opensearch.testutils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.opensearch.OpensearchSinkFunction;
import org.apache.flink.streaming.connectors.opensearch.RequestIndexer;

import org.junit.Assert;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains utilities and a pre-defined source function and Opensearch Sink function used
 * to simulate and verify data used in tests.
 */
public class SourceSinkDataTestKit {

    private static final int NUM_ELEMENTS = 20;

    private static final String DATA_PREFIX = "message #";
    private static final String DATA_FIELD_NAME = "data";

    /**
     * A {@link SourceFunction} that generates the elements (id, "message #" + id) with id being 0 -
     * 20.
     */
    public static class TestDataSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceFunction.SourceContext<Tuple2<Integer, String>> ctx)
                throws Exception {
            for (int i = 0; i < NUM_ELEMENTS && running; i++) {
                ctx.collect(Tuple2.of(i, DATA_PREFIX + i));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static OpensearchSinkFunction<Tuple2<Integer, String>> getJsonSinkFunction(
            String index) {
        return new TestOpensearchSinkFunction(index, XContentFactory::jsonBuilder);
    }

    public static OpensearchSinkFunction<Tuple2<Integer, String>> getSmileSinkFunction(
            String index) {
        return new TestOpensearchSinkFunction(index, XContentFactory::smileBuilder);
    }

    private static class TestOpensearchSinkFunction
            implements OpensearchSinkFunction<Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;

        private final String index;
        private final XContentBuilderProvider contentBuilderProvider;

        /**
         * Create the sink function, specifying a target Opensearch index.
         *
         * @param index Name of the target Opensearch index.
         */
        public TestOpensearchSinkFunction(
                String index, XContentBuilderProvider contentBuilderProvider) {
            this.index = index;
            this.contentBuilderProvider = contentBuilderProvider;
        }

        public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
            Map<String, Object> document = new HashMap<>();
            document.put(DATA_FIELD_NAME, element.f1);

            try {
                return new IndexRequest(index)
                        .id(element.f0.toString())
                        .source(contentBuilderProvider.getBuilder().map(document));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void process(
                Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(element));
        }
    }

    /**
     * Verify the results in an Opensearch index. The results must first be produced into the index
     * using a {@link TestOpensearchSinkFunction};
     *
     * @param client The client to use to connect to Opensearch
     * @param index The index to check
     * @throws IOException IOException
     */
    public static void verifyProducedSinkData(RestHighLevelClient client, String index)
            throws IOException {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            GetResponse response =
                    client.get(new GetRequest(index, Integer.toString(i)), RequestOptions.DEFAULT);
            Assert.assertEquals(DATA_PREFIX + i, response.getSource().get(DATA_FIELD_NAME));
        }
    }

    @FunctionalInterface
    private interface XContentBuilderProvider extends Serializable {
        XContentBuilder getBuilder() throws IOException;
    }
}
