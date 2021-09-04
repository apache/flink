package org.apache.flink.streaming.connectors.elasticsearch;

/**
 * This interface is responsible for defining routing and sink creation from an {@link ElementT} to
 * be consumed by an {@link DynamicElasticsearchSink} instance.
 *
 * <p>Example:
 *
 * <pre>{@code
 *           final ElasticsearchSinkRouter<> sinkRouter =
 *                  new ElasticsearchSinkRouter<
 *                          Tuple2<HttpHost, String>,
 *                          String,
 *                          ElasticsearchSink<Tuple2<HttpHost, String>>>() {
 *
 *                      @Override
 *                      public String getRoute(Tuple2<HttpHost, String> element) {
 *                          return element.f0.toHostString();
 *                      }
 *
 *                      @Override
 *                      public ElasticsearchSink<Tuple2<HttpHost, String>> createSink(
 *                              String cacheKey, Tuple2<HttpHost, String> element) {
 *
 *                          ElasticsearchSink.Builder<Tuple2<HttpHost, String>> builder =
 *                                  ElasticsearchSink.Builder(
 *                                      List.of(element.f0),
 *                                      (ElasticsearchSinkFunction<Tuple2<HttpHost, String>>)
 *                                          (el, ctx, indexer) -> {
 *                                              indexer.add(...);
 *                                          });
 *
 *                          // Add additional sink configuration here
 *                          return builder.build();
 *                      }
 *                  };
 * }</pre>
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
