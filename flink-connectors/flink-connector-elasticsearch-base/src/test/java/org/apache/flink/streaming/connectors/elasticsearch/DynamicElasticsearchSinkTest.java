package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBaseTest.DummyElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.elasticsearch.client.Requests;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Suite of tests for {@link DynamicElasticsearchSink}. */
public class DynamicElasticsearchSinkTest {

    /**
     * Tests that multiple elements that resolve to different routes via the router are each queued
     * to be sent by their respective underlying sink
     */
    @Test
    public void testItemsWithDifferentRoutesAreRoutedToRespectiveSinks() throws Exception {
        final DummyDynamicElasticsearchSink sink =
                new DummyDynamicElasticsearchSink(new DummyElasticsearchSinkRouter());

        final OneInputStreamOperatorTestHarness<Tuple2<String, String>, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-a", "1")));
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-b", "a")));

        assertEquals(1, sink.sinkRoutes.get("sink-a").getNumPendingRequests());
        assertEquals(1, sink.sinkRoutes.get("sink-b").getNumPendingRequests());
    }

    /**
     * Tests that for any items that resolve to the same route, that only a single sink for that
     * route is created (and cached) by the dynamic sink
     */
    @Test
    public void testItemsWithSameRouteReuseSameSink() throws Exception {
        final DummyDynamicElasticsearchSink sink =
                new DummyDynamicElasticsearchSink(new DummyElasticsearchSinkRouter());

        final OneInputStreamOperatorTestHarness<Tuple2<String, String>, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-a", "1")));
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-a", "2")));

        assertEquals(1, sink.sinkRoutes.size());
    }

    /**
     * Tests that for any items that resolve to different sinks that a unique one is created for
     * each route and will be reused for subsequent items that match known routes
     */
    @Test
    public void testItemsWithUniqueRoutesCreateSeparateSinks() throws Exception {
        final DummyDynamicElasticsearchSink sink =
                new DummyDynamicElasticsearchSink(new DummyElasticsearchSinkRouter());

        final OneInputStreamOperatorTestHarness<Tuple2<String, String>, Object> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-a", "1")));
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-b", "1")));
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-a", "2")));
        testHarness.processElement(new StreamRecord<>(Tuple2.of("sink-b", "2")));

        assertEquals(2, sink.sinkRoutes.size());
    }

    private static class DummyDynamicElasticsearchSink
            extends DynamicElasticsearchSink<
                    Tuple2<String, String>,
                    String,
                    DummyElasticsearchSink<Tuple2<String, String>>> {
        public DummyDynamicElasticsearchSink(
                ElasticsearchSinkRouter<
                                Tuple2<String, String>,
                                String,
                                DummyElasticsearchSink<Tuple2<String, String>>>
                        sinkRouter) {
            super(sinkRouter);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void invoke(Tuple2<String, String> value, Context context) throws Exception {
            super.invoke(value, context);
        }
    }

    private static class DummyElasticsearchSinkRouter
            implements ElasticsearchSinkRouter<
                    Tuple2<String, String>,
                    String,
                    DummyElasticsearchSink<Tuple2<String, String>>> {

        @Override
        public String getRoute(Tuple2<String, String> element) {
            return element.f0;
        }

        @Override
        public DummyElasticsearchSink<Tuple2<String, String>> createSink(
                String cacheKey, Tuple2<String, String> element) {

            return new DummyElasticsearchSink<>(
                    new HashMap<>(), new DummySinkFunction(), new NoOpFailureHandler());
        }
    }

    private static class DummySinkFunction
            implements ElasticsearchSinkFunction<Tuple2<String, String>> {
        @Override
        public void process(
                Tuple2<String, String> element, RuntimeContext ctx, RequestIndexer indexer) {

            Map<java.lang.String, Object> json = new HashMap<>();
            json.put("data", element);

            indexer.add(Requests.indexRequest().index("index").type("type").id("id").source(json));
        }
    }
}
