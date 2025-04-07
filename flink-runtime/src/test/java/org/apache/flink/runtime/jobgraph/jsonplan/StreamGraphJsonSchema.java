/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph.jsonplan;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/** A utility class for deserializing the JSON string of a stream graph. */
public class StreamGraphJsonSchema {
    public static final String FIELD_NAME_NODES = "nodes";

    @JsonProperty(FIELD_NAME_NODES)
    private final List<JsonStreamNodeSchema> nodes;

    @JsonCreator
    public StreamGraphJsonSchema(@JsonProperty(FIELD_NAME_NODES) List<JsonStreamNodeSchema> nodes) {
        this.nodes = nodes;
    }

    @JsonIgnore
    public List<JsonStreamNodeSchema> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamGraphJsonSchema that = (StreamGraphJsonSchema) o;
        return Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes);
    }

    public static class JsonStreamNodeSchema {
        public static final String FIELD_NAME_NODE_ID = "id";
        public static final String FIELD_NAME_NODE_PARALLELISM = "parallelism";
        public static final String FIELD_NAME_NODE_OPERATOR = "operator";
        public static final String FIELD_NAME_NODE_DESCRIPTION = "description";
        public static final String FIELD_NAME_NODE_JOB_VERTEX_ID = "job_vertex_id";
        public static final String FIELD_NAME_NODE_INPUTS = "inputs";

        @JsonProperty(FIELD_NAME_NODE_ID)
        private final String id;

        @JsonProperty(FIELD_NAME_NODE_PARALLELISM)
        private final Integer parallelism;

        @JsonProperty(FIELD_NAME_NODE_OPERATOR)
        private final String operator;

        @JsonProperty(FIELD_NAME_NODE_DESCRIPTION)
        private final String description;

        @JsonProperty(FIELD_NAME_NODE_JOB_VERTEX_ID)
        private final String jobVertexId;

        @JsonProperty(FIELD_NAME_NODE_INPUTS)
        private final List<JsonStreamEdgeSchema> inputs;

        @JsonCreator
        public JsonStreamNodeSchema(
                @JsonProperty(FIELD_NAME_NODE_ID) String id,
                @JsonProperty(FIELD_NAME_NODE_PARALLELISM) Integer parallelism,
                @JsonProperty(FIELD_NAME_NODE_OPERATOR) String operator,
                @JsonProperty(FIELD_NAME_NODE_DESCRIPTION) String description,
                @JsonProperty(FIELD_NAME_NODE_JOB_VERTEX_ID) String jobVertexId,
                @JsonProperty(FIELD_NAME_NODE_INPUTS) List<JsonStreamEdgeSchema> inputs) {
            this.id = id;
            this.parallelism = parallelism;
            this.operator = operator;
            this.description = description;
            this.jobVertexId = jobVertexId;
            this.inputs = inputs;
        }

        @JsonIgnore
        public String getId() {
            return id;
        }

        @JsonIgnore
        public Integer getParallelism() {
            return parallelism;
        }

        @JsonIgnore
        public String getOperator() {
            return operator;
        }

        @JsonIgnore
        public String getDescription() {
            return description;
        }

        @JsonIgnore
        public String getJobVertexId() {
            return jobVertexId;
        }

        @JsonIgnore
        public List<JsonStreamEdgeSchema> getInputs() {
            return inputs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JsonStreamNodeSchema that = (JsonStreamNodeSchema) o;
            return Objects.equals(id, that.id)
                    && Objects.equals(parallelism, that.parallelism)
                    && Objects.equals(operator, that.operator)
                    && Objects.equals(description, that.description)
                    && Objects.equals(jobVertexId, that.jobVertexId)
                    && Objects.equals(inputs, that.inputs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, parallelism, operator, description, jobVertexId, inputs);
        }
    }

    public static class JsonStreamEdgeSchema {
        public static final String FIELD_NAME_EDGE_INPUT_NUM = "num";
        public static final String FIELD_NAME_EDGE_ID = "id";
        public static final String FIELD_NAME_EDGE_SHIP_STRATEGY = "ship_strategy";
        public static final String FIELD_NAME_EDGE_EXCHANGE = "exchange";

        @JsonProperty(FIELD_NAME_EDGE_INPUT_NUM)
        private final Integer num;

        @JsonProperty(FIELD_NAME_EDGE_ID)
        private final String id;

        @JsonProperty(FIELD_NAME_EDGE_SHIP_STRATEGY)
        private final String shipStrategy;

        @JsonProperty(FIELD_NAME_EDGE_EXCHANGE)
        private final String exchange;

        @JsonCreator
        public JsonStreamEdgeSchema(
                @JsonProperty(FIELD_NAME_EDGE_INPUT_NUM) Integer num,
                @JsonProperty(FIELD_NAME_EDGE_ID) String id,
                @JsonProperty(FIELD_NAME_EDGE_SHIP_STRATEGY) String shipStrategy,
                @JsonProperty(FIELD_NAME_EDGE_EXCHANGE) String exchange) {
            this.num = num;
            this.id = id;
            this.shipStrategy = shipStrategy;
            this.exchange = exchange;
        }

        @JsonIgnore
        public Integer getNum() {
            return num;
        }

        @JsonIgnore
        public String getId() {
            return id;
        }

        @JsonIgnore
        public String getShipStrategy() {
            return shipStrategy;
        }

        @JsonIgnore
        public String getExchange() {
            return exchange;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JsonStreamEdgeSchema that = (JsonStreamEdgeSchema) o;
            return Objects.equals(num, that.num)
                    && Objects.equals(id, that.id)
                    && Objects.equals(shipStrategy, that.shipStrategy)
                    && Objects.equals(exchange, that.exchange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(num, id, shipStrategy, exchange);
        }
    }
}
