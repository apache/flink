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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Response type for a collection of metrics.
 *
 * <p>As JSON this type will be represented as an array of
 * metrics, i.e., the field <code>metrics</code> will not show up. For example, a collection with a
 * single metric will be represented as follows:
 * <pre>
 * {@code
 * [{"id": "metricName", "value": "1"}]
 * }
 * </pre>
 *
 * @see Serializer
 * @see Deserializer
 * @see org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore
 */
@JsonSerialize(using = MetricCollectionResponseBody.Serializer.class)
@JsonDeserialize(using = MetricCollectionResponseBody.Deserializer.class)
public final class MetricCollectionResponseBody implements ResponseBody {

	private final Collection<Metric> metrics;

	public MetricCollectionResponseBody(Collection<Metric> metrics) {
		this.metrics = requireNonNull(metrics, "metrics must not be null");
	}

	public Collection<Metric> getMetrics() {
		return metrics;
	}

	/**
	 * JSON serializer for {@link MetricCollectionResponseBody}.
	 */
	public static class Serializer extends StdSerializer<MetricCollectionResponseBody> {

		private static final long serialVersionUID = 1L;

		protected Serializer() {
			super(MetricCollectionResponseBody.class);
		}

		@Override
		public void serialize(
				MetricCollectionResponseBody metricCollectionResponseBody,
				JsonGenerator jsonGenerator,
				SerializerProvider serializerProvider) throws IOException {

			jsonGenerator.writeObject(metricCollectionResponseBody.getMetrics());
		}
	}

	/**
	 * JSON deserializer for {@link MetricCollectionResponseBody}.
	 */
	public static class Deserializer extends StdDeserializer<MetricCollectionResponseBody> {

		private static final long serialVersionUID = 1L;

		protected Deserializer() {
			super(MetricCollectionResponseBody.class);
		}

		@Override
		public MetricCollectionResponseBody deserialize(
				JsonParser jsonParser,
				DeserializationContext deserializationContext) throws IOException {

			return new MetricCollectionResponseBody(jsonParser.readValueAs(
				new TypeReference<List<Metric>>() {
				}));
		}
	}

}

