package org.apache.flink.streaming.connectors.elasticsearch;

/**
 * This interface is responsible for defining routing and sink creation from an {@link ElementT} to
 * be consumed by an {@link DynamicElasticsearchSink} instance.
 */
public interface ElasticsearchSinkRouter<
        ElementT, RouteT, SinkT extends ElasticsearchSinkBase<ElementT, ?>> {
    /**
     * Generate a deterministically unique identifier from an {@link ElementT} instance, which is
     * used to initialize and cache the {@link SinkT} defined in the router.
     */
    RouteT getRoute(ElementT element);

    /**
     * Generate a {@link SinkT} instance from an {@link ElementT} instance that will associate all
     * Elasticsearch requests for the {@link RouteT} defined in the router.
     */
    SinkT createSink(RouteT cacheKey, ElementT element);
}
