package org.apache.flink.streaming.connectors.elasticsearch5;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;

import org.elasticsearch.client.transport.TransportClient;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * IT cases for the {@link ElasticSearch5InputFormat}.
 */
public class Elasticsearch5InputFormatITCase extends ElasticsearchSinkTestBase<TransportClient, InetSocketAddress> {
	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, TransportClient> createElasticsearchSink(int bulkFlushMaxActions, String clusterName, List<InetSocketAddress> addresses, ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {
		return null;
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, TransportClient> createElasticsearchSinkForEmbeddedNode(int bulkFlushMaxActions, String clusterName, ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) throws Exception {
		return null;
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, TransportClient> createElasticsearchSinkForNode(int bulkFlushMaxActions, String clusterName, ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction, String ipAddress) throws Exception {
		return null;
	}
}
