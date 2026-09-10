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

package org.apache.flink.types.variant;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * A pull-based source of JSON tokens for building a {@link Variant} without a String round-trip.
 *
 * <p>A caller that already holds parsed JSON can feed tokens straight into the builder instead of
 * serializing back to a String only to have it parsed again. The interface exposes only JDK types
 * and the {@link Token} enum, so it stays portable across shading boundaries. Numbers are reported
 * as a single {@link Token#VALUE_NUMBER} carrying a raw literal; the builder classifies them.
 */
@Internal
public interface JsonTokenSource {

    /** Advances to and returns the next token, or {@code null} at end of input. */
    Token next() throws IOException;

    /** Returns the name of the current {@link Token#FIELD_NAME} token. */
    String fieldName() throws IOException;

    /** Returns the value of the current {@link Token#VALUE_STRING} token. */
    String stringValue() throws IOException;

    /** Returns the raw literal of the current {@link Token#VALUE_NUMBER} token. */
    String numberText() throws IOException;

    /**
     * A description of the current position in the input, used to enrich parse-error messages, or
     * {@code null} if the source cannot provide one.
     */
    default String currentLocation() {
        return null;
    }

    /** The kinds of tokens a {@link JsonTokenSource} can produce. */
    @Internal
    enum Token {
        START_OBJECT,
        END_OBJECT,
        START_ARRAY,
        END_ARRAY,
        FIELD_NAME,
        VALUE_STRING,
        VALUE_NUMBER,
        VALUE_TRUE,
        VALUE_FALSE,
        VALUE_NULL
    }
}
