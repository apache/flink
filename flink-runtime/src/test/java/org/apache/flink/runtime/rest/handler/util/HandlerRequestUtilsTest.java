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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HandlerRequestUtils}. */
class HandlerRequestUtilsTest {

    @Test
    void testGetQueryParameter() throws Exception {
        final Boolean queryParameter =
                HandlerRequestUtils.getQueryParameter(
                        HandlerRequest.resolveParametersAndCreate(
                                EmptyRequestBody.getInstance(),
                                new TestMessageParameters(),
                                Collections.emptyMap(),
                                Collections.singletonMap("key", Collections.singletonList("true")),
                                Collections.emptyList()),
                        TestBooleanQueryParameter.class);
        assertThat(queryParameter).isTrue();
    }

    @Test
    void testGetQueryParameterRepeated() throws Exception {
        try {
            HandlerRequestUtils.getQueryParameter(
                    HandlerRequest.resolveParametersAndCreate(
                            EmptyRequestBody.getInstance(),
                            new TestMessageParameters(),
                            Collections.emptyMap(),
                            Collections.singletonMap("key", Arrays.asList("true", "false")),
                            Collections.emptyList()),
                    TestBooleanQueryParameter.class);
        } catch (final RestHandlerException e) {
            assertThat(e.getMessage()).contains("Expected only one value");
        }
    }

    @Test
    void testGetQueryParameterDefaultValue() throws Exception {
        final Boolean allowNonRestoredState =
                HandlerRequestUtils.getQueryParameter(
                        HandlerRequest.resolveParametersAndCreate(
                                EmptyRequestBody.getInstance(),
                                new TestMessageParameters(),
                                Collections.emptyMap(),
                                Collections.singletonMap("key", Collections.emptyList()),
                                Collections.emptyList()),
                        TestBooleanQueryParameter.class,
                        true);
        assertThat(allowNonRestoredState).isTrue();
    }

    private static class TestMessageParameters extends MessageParameters {

        private final TestBooleanQueryParameter testBooleanQueryParameter =
                new TestBooleanQueryParameter();

        @Override
        public Collection<MessagePathParameter<?>> getPathParameters() {
            return Collections.emptyList();
        }

        @Override
        public Collection<MessageQueryParameter<?>> getQueryParameters() {
            return Collections.singletonList(testBooleanQueryParameter);
        }
    }

    private static class TestBooleanQueryParameter extends MessageQueryParameter<Boolean> {

        private TestBooleanQueryParameter() {
            super("key", MessageParameterRequisiteness.OPTIONAL);
        }

        @Override
        public Boolean convertStringToValue(final String value) {
            return Boolean.parseBoolean(value);
        }

        @Override
        public String convertValueToString(final Boolean value) {
            return value.toString();
        }

        @Override
        public String getDescription() {
            return "boolean query parameter";
        }
    }
}
