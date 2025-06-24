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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

/** This class contains utilities for mapping requests and responses to/from JSON. */
public class RestMapperUtils {
    private static final ObjectMapper strictObjectMapper;
    private static final ObjectMapper flexibleObjectMapper;

    static {
        strictObjectMapper = JacksonMapperFactory.createObjectMapper();
        strictObjectMapper.enable(
                DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,
                DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        strictObjectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        flexibleObjectMapper = strictObjectMapper.copy();
        flexibleObjectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Returns a preconfigured strict {@link ObjectMapper}.
     *
     * @return preconfigured object mapper
     */
    public static ObjectMapper getStrictObjectMapper() {
        return strictObjectMapper;
    }

    /**
     * Returns a preconfigured flexible {@link ObjectMapper}.
     *
     * @return preconfigured object mapper
     */
    public static ObjectMapper getFlexibleObjectMapper() {
        return flexibleObjectMapper;
    }
}
