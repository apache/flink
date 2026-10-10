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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/** Response body for batch aggregated subtask metrics grouped by job vertex. */
@JsonSerialize(using = AggregatedSubtaskMetricsBatchResponseBody.Serializer.class)
@JsonDeserialize(using = AggregatedSubtaskMetricsBatchResponseBody.Deserializer.class)
public class AggregatedSubtaskMetricsBatchResponseBody implements ResponseBody {

    private final Collection<VertexAggregatedMetrics> metricsByVertex;

    public AggregatedSubtaskMetricsBatchResponseBody(
            Collection<VertexAggregatedMetrics> metricsByVertex) {
        this.metricsByVertex =
                new ArrayList<>(
                        Preconditions.checkNotNull(
                                metricsByVertex, "metricsByVertex must not be null"));
    }

    @JsonIgnore
    public Collection<VertexAggregatedMetrics> getMetricsByVertex() {
        return metricsByVertex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregatedSubtaskMetricsBatchResponseBody that =
                (AggregatedSubtaskMetricsBatchResponseBody) o;
        return Objects.equals(metricsByVertex, that.metricsByVertex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricsByVertex);
    }

    /** JSON serializer for {@link AggregatedSubtaskMetricsBatchResponseBody}. */
    public static class Serializer
            extends StdSerializer<AggregatedSubtaskMetricsBatchResponseBody> {

        private static final long serialVersionUID = 1L;

        protected Serializer() {
            super(AggregatedSubtaskMetricsBatchResponseBody.class);
        }

        @Override
        public void serialize(
                AggregatedSubtaskMetricsBatchResponseBody response,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeObject(response.getMetricsByVertex());
        }
    }

    /** JSON deserializer for {@link AggregatedSubtaskMetricsBatchResponseBody}. */
    public static class Deserializer
            extends StdDeserializer<AggregatedSubtaskMetricsBatchResponseBody> {

        private static final long serialVersionUID = 1L;

        protected Deserializer() {
            super(AggregatedSubtaskMetricsBatchResponseBody.class);
        }

        @Override
        public AggregatedSubtaskMetricsBatchResponseBody deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            return new AggregatedSubtaskMetricsBatchResponseBody(
                    jsonParser.readValueAs(new TypeReference<List<VertexAggregatedMetrics>>() {}));
        }
    }

    /** Aggregated metrics for one job vertex. */
    public static class VertexAggregatedMetrics {

        private static final String FIELD_NAME_VERTEX_ID = "vertexId";
        private static final String FIELD_NAME_METRICS = "metrics";

        @JsonProperty(value = FIELD_NAME_VERTEX_ID, required = true)
        @JsonSerialize(using = JobVertexIDSerializer.class)
        @JsonDeserialize(using = JobVertexIDDeserializer.class)
        private final JobVertexID vertexId;

        @JsonProperty(value = FIELD_NAME_METRICS, required = true)
        private final Collection<AggregatedMetric> metrics;

        @JsonCreator
        public VertexAggregatedMetrics(
                @JsonProperty(value = FIELD_NAME_VERTEX_ID, required = true) JobVertexID vertexId,
                @JsonProperty(value = FIELD_NAME_METRICS, required = true)
                        Collection<AggregatedMetric> metrics) {
            this.vertexId = Preconditions.checkNotNull(vertexId, "vertexId must not be null");
            this.metrics =
                    new ArrayList<>(
                            Preconditions.checkNotNull(metrics, "metrics must not be null"));
        }

        @JsonIgnore
        public JobVertexID getVertexId() {
            return vertexId;
        }

        @JsonIgnore
        public Collection<AggregatedMetric> getMetrics() {
            return metrics;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VertexAggregatedMetrics that = (VertexAggregatedMetrics) o;
            return Objects.equals(vertexId, that.vertexId) && Objects.equals(metrics, that.metrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertexId, metrics);
        }
    }
}
