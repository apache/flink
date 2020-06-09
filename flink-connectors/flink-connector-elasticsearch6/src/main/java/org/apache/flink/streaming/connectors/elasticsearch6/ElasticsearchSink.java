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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Elasticsearch 6.x sink that requests multiple {@link ActionRequest ActionRequests}
 * against a cluster for each incoming element.
 *
 * <p>The sink internally uses a {@link RestHighLevelClient} to communicate with an Elasticsearch cluster.
 * The sink will fail if no cluster can be connected to using the provided transport addresses passed to the constructor.
 *
 * <p>Internally, the sink will use a {@link BulkProcessor} to send {@link ActionRequest ActionRequests}.
 * This will buffer elements before sending a request to the cluster. The behaviour of the
 * {@code BulkProcessor} can be configured using these config keys:
 * <ul>
 *   <li> {@code bulk.flush.max.actions}: Maximum amount of elements to buffer
 *   <li> {@code bulk.flush.max.size.mb}: Maximum amount of data (in megabytes) to buffer
 *   <li> {@code bulk.flush.interval.ms}: Interval at which to flush data regardless of the other two
 *   settings in milliseconds
 * </ul>
 *
 * <p>You also have to provide an {@link ElasticsearchSinkFunction}. This is used to create multiple
 * {@link ActionRequest ActionRequests} for each incoming element. See the class level documentation of
 * {@link ElasticsearchSinkFunction} for an example.
 *
 * @param <T> Type of the elements handled by this sink
 */
@PublicEvolving
public class ElasticsearchSink<T> extends ElasticsearchSinkBase<T, RestHighLevelClient> {

	private static final long serialVersionUID = 1L;

	private ElasticsearchSink(
		Map<String, String> bulkRequestsConfig,
		List<HttpHost> httpHosts,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		ActionRequestFailureHandler failureHandler,
		RestClientFactory restClientFactory) {

		super(new Elasticsearch6ApiCallBridge(httpHosts, restClientFactory),  bulkRequestsConfig, elasticsearchSinkFunction, failureHandler);
	}

	/**
	 * A builder for creating an {@link ElasticsearchSink}.
	 *
	 * @param <T> Type of the elements handled by the sink this builder creates.
	 */
	@PublicEvolving
	public static class Builder<T> {

		private final List<HttpHost> httpHosts;
		private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

		private Map<String, String> bulkRequestsConfig = new HashMap<>();
		private ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();
		private RestClientFactory restClientFactory = restClientBuilder -> {};

		/**
		 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
		 *
		 * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient} connects to.
		 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element.
		 */
		public Builder(List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
			this.httpHosts = Preconditions.checkNotNull(httpHosts);
			this.elasticsearchSinkFunction = Preconditions.checkNotNull(elasticsearchSinkFunction);
		}

		/**
		 * Sets the maximum number of actions to buffer for each bulk request.
		 *
		 * @param numMaxActions the maxinum number of actions to buffer per bulk request.
		 */
		public void setBulkFlushMaxActions(int numMaxActions) {
			this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(numMaxActions));
		}

		/**
		 * Sets the maximum size of buffered actions, in mb, per bulk request.
		 *
		 * @param maxSizeMb the maximum size of buffered actions, in mb.
		 */
		public void setBulkFlushMaxSizeMb(int maxSizeMb) {
			this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, String.valueOf(maxSizeMb));
		}

		/**
		 * Sets the bulk flush interval, in milliseconds.
		 *
		 * @param intervalMillis the bulk flush interval, in milliseconds.
		 */
		public void setBulkFlushInterval(long intervalMillis) {
			this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, String.valueOf(intervalMillis));
		}

		/**
		 * Sets whether or not to enable bulk flush backoff behaviour.
		 *
		 * @param enabled whether or not to enable backoffs.
		 */
		public void setBulkFlushBackoff(boolean enabled) {
			this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, String.valueOf(enabled));
		}

		/**
		 * Sets the type of back of to use when flushing bulk requests.
		 *
		 * @param flushBackoffType the backoff type to use.
		 */
		public void setBulkFlushBackoffType(FlushBackoffType flushBackoffType) {
			this.bulkRequestsConfig.put(
				CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE,
				Preconditions.checkNotNull(flushBackoffType).toString());
		}

		/**
		 * Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
		 *
		 * @param maxRetries the maximum number of retries for a backoff attempt when flushing bulk requests
		 */
		public void setBulkFlushBackoffRetries(int maxRetries) {
			Preconditions.checkArgument(
				maxRetries > 0,
				"Max number of backoff attempts must be larger than 0.");

			this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES, String.valueOf(maxRetries));
		}

		/**
		 * Sets the amount of delay between each backoff attempt when flushing bulk requests, in milliseconds.
		 *
		 * @param delayMillis the amount of delay between each backoff attempt when flushing bulk requests, in milliseconds.
		 */
		public void setBulkFlushBackoffDelay(long delayMillis) {
			Preconditions.checkArgument(
				delayMillis >= 0,
				"Delay (in milliseconds) between each backoff attempt must be larger than or equal to 0.");
			this.bulkRequestsConfig.put(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY, String.valueOf(delayMillis));
		}

		/**
		 * Sets a failure handler for action requests.
		 *
		 * @param failureHandler This is used to handle failed {@link ActionRequest}.
		 */
		public void setFailureHandler(ActionRequestFailureHandler failureHandler) {
			this.failureHandler = Preconditions.checkNotNull(failureHandler);
		}

		/**
		 * Sets a REST client factory for custom client configuration.
		 *
		 * @param restClientFactory the factory that configures the rest client.
		 */
		public void setRestClientFactory(RestClientFactory restClientFactory) {
			this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
		}

		/**
		 * Creates the Elasticsearch sink.
		 *
		 * @return the created Elasticsearch sink.
		 */
		public ElasticsearchSink<T> build() {
			return new ElasticsearchSink<>(bulkRequestsConfig, httpHosts, elasticsearchSinkFunction, failureHandler, restClientFactory);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Builder<?> builder = (Builder<?>) o;
			return Objects.equals(httpHosts, builder.httpHosts) &&
				Objects.equals(elasticsearchSinkFunction, builder.elasticsearchSinkFunction) &&
				Objects.equals(bulkRequestsConfig, builder.bulkRequestsConfig) &&
				Objects.equals(failureHandler, builder.failureHandler) &&
				Objects.equals(restClientFactory, builder.restClientFactory);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				httpHosts,
				elasticsearchSinkFunction,
				bulkRequestsConfig,
				failureHandler,
				restClientFactory);
		}
	}
}
