package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates a dynamic wrapper for the Elasticsearch API that supports routing {@link ElementT}
 * elements to corresponding {@link SinkT} instances based on the logic defined within a
 * {@link ElasticsearchSinkRouter}.
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

    private final ElasticsearchSinkRouter<ElementT, RouteT, SinkT> sinkRouter;
    private final Map<RouteT, SinkT> sinkRoutes = new HashMap<>();

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
}
