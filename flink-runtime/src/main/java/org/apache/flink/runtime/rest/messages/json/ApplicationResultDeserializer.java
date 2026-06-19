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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.runtime.highavailability.ApplicationResult;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * JSON deserializer for {@link ApplicationResult}.
 *
 * @see ApplicationResultSerializer
 */
public class ApplicationResultDeserializer extends StdDeserializer<ApplicationResult> {

    private static final long serialVersionUID = 1L;

    private final ApplicationIDDeserializer applicationIdDeserializer =
            new ApplicationIDDeserializer();

    private final SerializedThrowableDeserializer serializedThrowableDeserializer =
            new SerializedThrowableDeserializer();

    public ApplicationResultDeserializer() {
        super(ApplicationResult.class);
    }

    @Override
    public ApplicationResult deserialize(final JsonParser p, final DeserializationContext ctxt)
            throws IOException {
        ApplicationID applicationId = null;
        ApplicationState applicationState = null;
        String applicationName = "unknown";
        long startTime = -1;
        long endTime = -1;

        while (true) {
            final JsonToken jsonToken = p.nextToken();
            assertNotEndOfInput(p, jsonToken);
            if (jsonToken == JsonToken.END_OBJECT) {
                break;
            }

            final String fieldName = p.getValueAsString();
            switch (fieldName) {
                case ApplicationResultSerializer.FIELD_NAME_APPLICATION_ID:
                    assertNextToken(p, JsonToken.VALUE_STRING);
                    applicationId = applicationIdDeserializer.deserialize(p, ctxt);
                    break;
                case ApplicationResultSerializer.FIELD_NAME_APPLICATION_STATE:
                    assertNextToken(p, JsonToken.VALUE_STRING);
                    applicationState = ApplicationState.valueOf(p.getValueAsString());
                    break;
                case ApplicationResultSerializer.FIELD_NAME_APPLICATION_NAME:
                    assertNextToken(p, JsonToken.VALUE_STRING);
                    applicationName = p.getValueAsString();
                    break;
                case ApplicationResultSerializer.FIELD_NAME_START_TIME:
                    assertNextToken(p, JsonToken.VALUE_NUMBER_INT);
                    startTime = p.getLongValue();
                    break;
                case ApplicationResultSerializer.FIELD_NAME_END_TIME:
                    assertNextToken(p, JsonToken.VALUE_NUMBER_INT);
                    endTime = p.getLongValue();
                    break;
                default:
                    // ignore unknown fields
            }
        }

        try {
            return new ApplicationResult.Builder()
                    .applicationId(applicationId)
                    .applicationState(applicationState)
                    .applicationName(applicationName)
                    .startTime(startTime)
                    .endTime(endTime)
                    .build();
        } catch (final RuntimeException e) {
            throw new JsonMappingException(
                    null, "Could not deserialize " + ApplicationResult.class.getSimpleName(), e);
        }
    }

    /** Asserts that the provided JsonToken is not null, i.e., not at the end of the input. */
    private static void assertNotEndOfInput(
            final JsonParser p, @Nullable final JsonToken jsonToken) {
        checkState(jsonToken != null, "Unexpected end of input at %s", p.getCurrentLocation());
    }

    /** Advances the token and asserts that it matches the required {@link JsonToken}. */
    private static void assertNextToken(final JsonParser p, final JsonToken requiredJsonToken)
            throws IOException {
        final JsonToken jsonToken = p.nextToken();
        if (jsonToken != requiredJsonToken) {
            throw new JsonMappingException(
                    p, String.format("Expected token %s (was %s)", requiredJsonToken, jsonToken));
        }
    }
}
