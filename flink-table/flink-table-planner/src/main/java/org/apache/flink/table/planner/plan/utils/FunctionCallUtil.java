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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.rex.RexLiteral;

import java.util.Objects;

/** Common utils for function call, e.g. ML_PREDICT and Lookup Join. */
public abstract class FunctionCallUtil {

    /** A field used as an equal condition when querying content from a dimension table. */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Constant.class),
        @JsonSubTypes.Type(value = FieldRef.class)
    })
    public static class FunctionParam {
        private FunctionParam() {
            // sealed class
        }
    }

    /** A {@link FunctionParam} whose value is constant. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("Constant")
    public static class Constant extends FunctionParam {
        public static final String FIELD_NAME_SOURCE_TYPE = "sourceType";
        public static final String FIELD_NAME_LITERAL = "literal";

        @JsonProperty(FIELD_NAME_SOURCE_TYPE)
        public final LogicalType sourceType;

        @JsonProperty(FIELD_NAME_LITERAL)
        public final RexLiteral literal;

        @JsonCreator
        public Constant(
                @JsonProperty(FIELD_NAME_SOURCE_TYPE) LogicalType sourceType,
                @JsonProperty(FIELD_NAME_LITERAL) RexLiteral literal) {
            this.sourceType = sourceType;
            this.literal = literal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Constant that = (Constant) o;
            return Objects.equals(sourceType, that.sourceType)
                    && Objects.equals(literal, that.literal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceType, literal);
        }
    }

    /** A {@link FunctionParam} whose value comes from the left table field. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("FieldRef")
    public static class FieldRef extends FunctionParam {
        public static final String FIELD_NAME_INDEX = "index";

        @JsonProperty(FIELD_NAME_INDEX)
        public final int index;

        @JsonCreator
        public FieldRef(@JsonProperty(FIELD_NAME_INDEX) int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldRef that = (FieldRef) o;
            return index == that.index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }
    }

    /** AsyncLookupOptions includes async related options. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("AsyncOptions")
    public static class AsyncOptions {
        public static final String FIELD_NAME_CAPACITY = "capacity ";
        public static final String FIELD_NAME_TIMEOUT = "timeout";
        public static final String FIELD_NAME_OUTPUT_MODE = "output-mode";
        public static final String FIELD_NAME_IS_KEY_ORDERED = "is-key-ordered";

        @JsonProperty(FIELD_NAME_CAPACITY)
        public final int asyncBufferCapacity;

        @JsonProperty(FIELD_NAME_TIMEOUT)
        public final long asyncTimeout;

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        @JsonProperty(FIELD_NAME_IS_KEY_ORDERED)
        public final boolean keyOrdered;

        @JsonProperty(FIELD_NAME_OUTPUT_MODE)
        public final AsyncDataStream.OutputMode asyncOutputMode;

        @JsonCreator
        public AsyncOptions(
                @JsonProperty(FIELD_NAME_CAPACITY) int asyncBufferCapacity,
                @JsonProperty(FIELD_NAME_TIMEOUT) long asyncTimeout,
                @JsonProperty(FIELD_NAME_IS_KEY_ORDERED) boolean keyOrdered,
                @JsonProperty(FIELD_NAME_OUTPUT_MODE) AsyncDataStream.OutputMode asyncOutputMode) {
            this.asyncBufferCapacity = asyncBufferCapacity;
            this.asyncTimeout = asyncTimeout;
            this.keyOrdered = keyOrdered;
            this.asyncOutputMode = asyncOutputMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AsyncOptions that = (AsyncOptions) o;
            return asyncBufferCapacity == that.asyncBufferCapacity
                    && asyncTimeout == that.asyncTimeout
                    && keyOrdered == that.keyOrdered
                    && asyncOutputMode == that.asyncOutputMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(asyncBufferCapacity, asyncTimeout, keyOrdered, asyncOutputMode);
        }

        @Override
        public String toString() {
            return asyncOutputMode
                    + ", "
                    + "KEY_ORDERED: "
                    + keyOrdered
                    + ", "
                    + asyncTimeout
                    + "ms, "
                    + asyncBufferCapacity;
        }
    }

    public static <T> T coalesce(T t1, T t2) {
        return t1 != null ? t1 : t2;
    }

    protected static AsyncDataStream.OutputMode convert(
            ChangelogMode inputChangelogMode,
            ExecutionConfigOptions.AsyncOutputMode asyncOutputMode) {
        if (inputChangelogMode.containsOnly(RowKind.INSERT)
                && asyncOutputMode == ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED) {
            return AsyncDataStream.OutputMode.UNORDERED;
        }
        return AsyncDataStream.OutputMode.ORDERED;
    }
}
