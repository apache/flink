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

import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.TestTemplate;

import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test base for verifying that marshalling / unmarshalling REST {@link ResponseBody}s work
 * properly.
 */
public abstract class RestResponseMarshallingTestBase<R extends ResponseBody> {

    /**
     * Returns the class of the test response.
     *
     * @return class of the test response type
     */
    protected abstract Class<R> getTestResponseClass();

    protected Collection<Class<?>> getTypeParameters() {
        return Collections.emptyList();
    }

    /**
     * Returns an instance of a response to be tested.
     *
     * @return instance of the expected test response
     */
    protected abstract R getTestResponseInstance() throws Exception;

    /** Tests that we can marshal and unmarshal the response. */
    @TestTemplate
    public void testJsonMarshalling() throws Exception {
        final R expected = getTestResponseInstance();

        ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
        final String marshalled = objectMapper.writeValueAsString(expected);

        final Collection<Class<?>> typeParameters = getTypeParameters();
        final JavaType type;

        if (typeParameters.isEmpty()) {
            type = objectMapper.getTypeFactory().constructType(getTestResponseClass());
        } else {
            type =
                    objectMapper
                            .getTypeFactory()
                            .constructParametricType(
                                    getTestResponseClass(),
                                    typeParameters.toArray(new Class<?>[typeParameters.size()]));
        }

        final R unmarshalled = objectMapper.readValue(marshalled, type);
        assertOriginalEqualsToUnmarshalled(expected, unmarshalled);
    }

    /**
     * Asserts that two objects are equal. If they are not, an {@link AssertionError} is thrown.
     *
     * @param expected expected value
     * @param actual the value to check against expected
     */
    protected void assertOriginalEqualsToUnmarshalled(R expected, R actual) {
        assertThat(expected).isEqualTo(actual);
    }
}
