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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Objects;

/** Response type of the {@link JobPlanHandler}. */
public class JobPlanInfo implements ResponseBody {

    private static final String FIELD_NAME_PLAN = "plan";

    @JsonProperty(FIELD_NAME_PLAN)
    private final RawJson jsonPlan;

    public JobPlanInfo(String jsonPlan) {
        this(new RawJson(Preconditions.checkNotNull(jsonPlan)));
    }

    @JsonCreator
    public JobPlanInfo(@JsonProperty(FIELD_NAME_PLAN) RawJson jsonPlan) {
        this.jsonPlan = jsonPlan;
    }

    @JsonIgnore
    public String getJsonPlan() {
        return jsonPlan.json;
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
        return Objects.equals(jsonPlan, that.jsonPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonPlan);
    }

    @Override
    public String toString() {
        return "JobPlanInfo{" + "jsonPlan=" + jsonPlan + '}';
    }

    /** Simple wrapper around a raw JSON string. */
    @JsonSerialize(using = RawJson.Serializer.class)
    @JsonDeserialize(using = RawJson.Deserializer.class)
    public static final class RawJson {
        private final String json;

        private RawJson(String json) {
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
}
