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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.job.JobPlanHandler;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

/** Response type of the {@link JobPlanHandler}. */
public class JobPlanInfo implements ResponseBody {

    private static final String FIELD_NAME_PLAN = "plan";

    @JsonProperty(FIELD_NAME_PLAN)
    private final Plan plan;

    @JsonCreator
    public JobPlanInfo(@JsonProperty(FIELD_NAME_PLAN) Plan plan) {
        this.plan = plan;
    }

    @JsonIgnore
    public Plan getPlan() {
        return plan;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobPlanInfo that = (JobPlanInfo) o;
        return Objects.equals(plan, that.plan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan);
    }

    @Override
    public String toString() {
        return "JobPlanInfo{" + "plan=" + plan + '}';
    }

    /** Simple wrapper around a raw JSON string. */
    @JsonSerialize(using = RawJson.Serializer.class)
    @JsonDeserialize(using = RawJson.Deserializer.class)
    public static final class RawJson {
        private final String json;

        public RawJson(String json) {
            this.json = json;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RawJson rawJson = (RawJson) o;
            return Objects.equals(json, rawJson.json);
        }

        @Override
        public int hashCode() {
            return Objects.hash(json);
        }

        @Override
        public String toString() {
            return "RawJson{" + "json='" + json + '\'' + '}';
        }

        // ---------------------------------------------------------------------------------
        // Static helper classes
        // ---------------------------------------------------------------------------------

        /** Json serializer for the {@link RawJson}. */
        public static final class Serializer extends StdSerializer<RawJson> {

            private static final long serialVersionUID = -1551666039618928811L;

            public Serializer() {
                super(RawJson.class);
            }

            @Override
            public void serialize(
                    RawJson jobPlanInfo,
                    JsonGenerator jsonGenerator,
                    SerializerProvider serializerProvider)
                    throws IOException {
                jsonGenerator.writeRawValue(jobPlanInfo.json);
            }

            @Override
            public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType typeHint)
                    throws JsonMappingException {
                // this ensures the type is documented as "object" in the documentation
                visitor.expectObjectFormat(typeHint);
            }
        }

        /** Json deserializer for the {@link RawJson}. */
        public static final class Deserializer extends StdDeserializer<RawJson> {

            private static final long serialVersionUID = -3580088509877177213L;

            public Deserializer() {
                super(RawJson.class);
            }

            @Override
            public RawJson deserialize(
                    JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException {
                final JsonNode rootNode = jsonParser.readValueAsTree();
                return new RawJson(rootNode.toString());
            }
        }
    }

    // ---------------------------------------------------
    // Static inner classes
    // ---------------------------------------------------

    /** An inner class for the plan reference. */
    @Schema(name = "Plan")
    public static final class Plan implements Serializable {
        private static final String FIELD_NAME_JOB_ID = "jid";

        @JsonProperty(FIELD_NAME_JOB_ID)
        private final String jobId;

        private static final String FIELD_NAME_NAME = "name";

        @JsonProperty(FIELD_NAME_NAME)
        private final String name;

        private static final String FIELD_NAME_TYPE = "type";

        @JsonProperty(FIELD_NAME_TYPE)
        private final String type;

        private static final String FIELD_NAME_NODES = "nodes";

        @JsonProperty(FIELD_NAME_NODES)
        private final Collection<Node> nodes;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Plan that = (Plan) o;
            return Objects.equals(jobId, that.jobId)
                    && Objects.equals(name, that.name)
                    && Objects.equals(type, that.type)
                    && Objects.equals(nodes, that.nodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, name, type, nodes);
        }

        @JsonCreator
        public Plan(
                @JsonProperty(FIELD_NAME_JOB_ID) String jobId,
                @JsonProperty(FIELD_NAME_NAME) String name,
                @JsonProperty(FIELD_NAME_TYPE) String type,
                @JsonProperty(FIELD_NAME_NODES) Collection<Node> nodes) {
            this.jobId = Preconditions.checkNotNull(jobId);
            this.name = Preconditions.checkNotNull(name);
            this.type = Preconditions.checkNotNull(type);
            this.nodes = Preconditions.checkNotNull(nodes);
        }

        @JsonIgnore
        public String getJobId() {
            return jobId;
        }

        @JsonIgnore
        public String getName() {
            return name;
        }

        @JsonIgnore
        public String getType() {
            return type;
        }

        @JsonIgnore
        public Collection<Node> getNodes() {
            return nodes;
        }

        /** An inner class containing node (vertex) level info. */
        @Schema(name = "PlanNode")
        public static final class Node implements Serializable {
            private static final String FIELD_NAME_ID = "id";

            @JsonProperty(FIELD_NAME_ID)
            private final String id;

            private static final String FIELD_NAME_PARALLELISM = "parallelism";

            @JsonProperty(FIELD_NAME_PARALLELISM)
            private final long parallelism;

            private static final String FIELD_NAME_OPERATOR = "operator";

            @JsonProperty(FIELD_NAME_OPERATOR)
            private final String operator;

            private static final String FIELD_NAME_OPERATOR_STRATEGY = "operator_strategy";

            @JsonProperty(FIELD_NAME_OPERATOR_STRATEGY)
            private final String operatorStrategy;

            private static final String FIELD_NAME_DESCRIPTION = "description";

            @JsonProperty(FIELD_NAME_DESCRIPTION)
            private final String description;

            private static final String FIELD_NAME_OPTIMIZER_PROPERTIES = "optimizer_properties";

            @JsonProperty(FIELD_NAME_OPTIMIZER_PROPERTIES)
            private final String optimizerProperties;

            private static final String FIELD_NAME_INPUTS = "inputs";

            @JsonProperty(FIELD_NAME_INPUTS)
            private final Collection<Input> inputs;

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Node that = (Node) o;
                return Objects.equals(id, that.id)
                        && Objects.equals(operator, that.operator)
                        && Objects.equals(operatorStrategy, that.operatorStrategy)
                        && Objects.equals(description, that.description)
                        && Objects.equals(optimizerProperties, that.optimizerProperties)
                        && Objects.equals(inputs, that.inputs)
                        && parallelism == that.parallelism;
            }

            @Override
            public int hashCode() {
                return Objects.hash(
                        id,
                        operator,
                        parallelism,
                        operatorStrategy,
                        description,
                        optimizerProperties,
                        inputs);
            }

            @JsonCreator
            public Node(
                    @JsonProperty(FIELD_NAME_ID) String id,
                    @JsonProperty(FIELD_NAME_OPERATOR) String operator,
                    @JsonProperty(FIELD_NAME_PARALLELISM) long parallelism,
                    @JsonProperty(FIELD_NAME_OPERATOR_STRATEGY) String operatorStrategy,
                    @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
                    @JsonProperty(FIELD_NAME_OPTIMIZER_PROPERTIES) String optimizerProperties,
                    @JsonProperty(FIELD_NAME_INPUTS) Collection<Input> inputs) {
                this.id = Preconditions.checkNotNull(id);
                this.operator = Preconditions.checkNotNull(operator);
                this.parallelism = Preconditions.checkNotNull(parallelism);
                this.operatorStrategy = Preconditions.checkNotNull(operatorStrategy);
                this.description = Preconditions.checkNotNull(description);
                this.optimizerProperties = Preconditions.checkNotNull(optimizerProperties);
                this.inputs = Preconditions.checkNotNull(inputs);
            }

            @JsonIgnore
            public String getId() {
                return id;
            }

            @JsonIgnore
            public long getParallelism() {
                return parallelism;
            }

            @JsonIgnore
            public String getOperator() {
                return operator;
            }

            @JsonIgnore
            public String getOperatorStrategy() {
                return operatorStrategy;
            }

            @JsonIgnore
            public String getDescription() {
                return description;
            }

            @JsonIgnore
            public String getOptimizerProperties() {
                return optimizerProperties;
            }

            @JsonIgnore
            public Collection<Input> getInputs() {
                return inputs;
            }

            /**
             * An inner class containing information on what input nodes should be linked to this
             * node.
             */
            @Schema(name = "Input")
            public static final class Input implements Serializable {
                private static final String FIELD_NAME_NUM = "num";

                @JsonProperty(FIELD_NAME_NUM)
                private final long num;

                private static final String FIELD_NAME_ID = "id";

                @JsonProperty(FIELD_NAME_ID)
                private final String id;

                private static final String FIELD_NAME_SHIP_STRATEGY = "ship_strategy";

                @Nullable
                @JsonProperty(FIELD_NAME_SHIP_STRATEGY)
                private final String shipStrategy;

                private static final String FIELD_NAME_LOCAL_STRATEGY = "local_strategy";

                @Nullable
                @JsonProperty(FIELD_NAME_LOCAL_STRATEGY)
                private final String localStrategy;

                private static final String FIELD_NAME_CACHING = "caching";

                @Nullable
                @JsonProperty(FIELD_NAME_CACHING)
                private final String caching;

                private static final String FIELD_NAME_EXCHANGE = "exchange";

                @JsonProperty(FIELD_NAME_EXCHANGE)
                private final String exchange;

                @Override
                public boolean equals(Object o) {
                    if (this == o) {
                        return true;
                    }
                    if (o == null || getClass() != o.getClass()) {
                        return false;
                    }
                    Input that = (Input) o;
                    return Objects.equals(id, that.id)
                            && num == that.num
                            && Objects.equals(shipStrategy, that.shipStrategy)
                            && Objects.equals(localStrategy, that.localStrategy)
                            && Objects.equals(caching, that.caching)
                            && Objects.equals(exchange, that.exchange);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(id, num, shipStrategy, localStrategy, caching, exchange);
                }

                @JsonCreator
                public Input(
                        @JsonProperty(FIELD_NAME_ID) String id,
                        @JsonProperty(FIELD_NAME_NUM) long num,
                        @JsonProperty(FIELD_NAME_EXCHANGE) String exchange,
                        @Nullable @JsonProperty(FIELD_NAME_SHIP_STRATEGY) String shipStrategy,
                        @Nullable @JsonProperty(FIELD_NAME_LOCAL_STRATEGY) String localStrategy,
                        @Nullable @JsonProperty(FIELD_NAME_CACHING) String caching) {
                    this.id = Preconditions.checkNotNull(id);
                    this.num = Preconditions.checkNotNull(num);
                    this.exchange = Preconditions.checkNotNull(exchange);
                    this.shipStrategy = shipStrategy;
                    this.localStrategy = localStrategy;
                    this.caching = caching;
                }

                @JsonIgnore
                public long getNum() {
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
                public String getLocalStrategy() {
                    return localStrategy;
                }

                @JsonIgnore
                public String getCaching() {
                    return caching;
                }

                @JsonIgnore
                public String getExchange() {
                    return exchange;
                }
            }
        }
    }
}
