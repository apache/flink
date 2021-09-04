package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates a dynamic wrapper for the Elasticsearch API that supports routing {@link ElementT}
 * elements to corresponding {@link SinkT} instances based on the logic defined within a {@link
 * ElasticsearchSinkRouter}.
 *
 * <p>Example:
 *
 * <pre>{@code
 *           // Define a series of Tuples where the first element represents the ES
 *           // host being targeted and the second is the message payload
 *           final DataStream<Tuple2<HttpHost, String>> stream = ...;
 *           stream
 *               .addSink(
 *                   new DynamicElasticsearchSink<>(
 *                       new ElasticsearchSinkRouter<
 *                               Tuple2<HttpHost, String>,
 *                               String,
 *                               ElasticsearchSink<Tuple2<HttpHost, String>>>() {
 *
 *                           @Override
 *                           public String getRoute(Tuple2<HttpHost, String> element) {
 *                               return element.f0.toHostString();
 *                           }
 *
 *                           @Override
 *                           public ElasticsearchSink<Tuple2<HttpHost, String>> createSink(
 *                                   String cacheKey, Tuple2<HttpHost, String> element) {
 *
 *                               ElasticsearchSink.Builder<Tuple2<HttpHost, String>> builder =
 *                                       ElasticsearchSink.Builder(
 *                                           List.of(element.f0),
 *                                           (ElasticsearchSinkFunction<Tuple2<HttpHost, String>>)
 *                                               (el, ctx, indexer) -> {
 *                                                   // Construct index request.
 *                                                   indexer.add(...);
 *                                               });
 *
 *                               // Add additional sink configuration here
 *                               return builder.build();
 *                           }
 *                       }
 *                   )
 *               );
 * }</pre>
 *
 * @param <ElementT> The type of element being routed
 * @param <RouteT> The type of deterministic identifier used to associate an element to a sink
 * @param <SinkT> The sink that the element will be written to
 */
public class DynamicElasticsearchSink<
                ElementT,
                RouteT,
                SinkT extends ElasticsearchSinkBase<ElementT, ? extends AutoCloseable>>
        extends RichSinkFunction<ElementT> implements CheckpointedFunction {

    /**
     * The {@link ElasticsearchSinkRouter} that will defined how to identify a unique {@link RouteT}
     * route from an incoming {@link ElementT} and ensure that all elements for that route are sent
     * to the corresponding {@link SinkT} defined.
     */
    private final ElasticsearchSinkRouter<ElementT, RouteT, SinkT> sinkRouter;

    /**
     * A key-value cache of all of the previously seen route-sink combinations to reduce the
     * overhead of creating a new sink each time for routes that have already been established.
     *
     * <p>NOTE: Since individual sink instances will be stored in memory, it is important that any
     * usage of this should properly allocate adequate memory if use-cases might result in the
     * creation of a large number of separate sinks.
     */
    protected final Map<RouteT, SinkT> sinkRoutes = new HashMap<>();

    private transient Configuration configuration;

    public DynamicElasticsearchSink(ElasticsearchSinkRouter<ElementT, RouteT, SinkT> sinkRouter) {
        this.sinkRouter = sinkRouter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.configuration = parameters;
    }

    @Override
    public void invoke(ElementT value, Context context) throws Exception {
        final RouteT route = sinkRouter.getRoute(value);
        SinkT sink = sinkRoutes.get(route);
        if (sink == null) {
            // If a route wasn't found for the existing element, create a new sink according
            // to the router definition and cache it
            sink = sinkRouter.createSink(route, value);
            sink.setRuntimeContext(getRuntimeContext());
            sink.open(configuration);
            sinkRoutes.put(route, sink);
        }

        sink.invoke(value, context);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // No-op.
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (SinkT sink : sinkRoutes.values()) {
            sink.snapshotState(context);
        }
    }

    @Override
    public void close() throws Exception {
        for (SinkT sink : sinkRoutes.values()) {
            sink.close();
        }
    }

    @Override
    public void finish() throws Exception {
        for (SinkT sink : sinkRoutes.values()) {
            sink.finish();
        }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        for (SinkT sink : sinkRoutes.values()) {
            sink.writeWatermark(watermark);
        }
    }
}
