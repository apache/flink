/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.jackson;

import org.apache.flink.annotation.Experimental;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** Factory for Jackson mappers. */
@Experimental
public final class JacksonMapperFactory {

    public static ObjectMapper createObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        registerModules(objectMapper);
        return objectMapper;
    }

    public static ObjectMapper createObjectMapper(JsonFactory jsonFactory) {
        final ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
        registerModules(objectMapper);
        return objectMapper;
    }

    public static CsvMapper createCsvMapper() {
        final CsvMapper csvMapper = new CsvMapper();
        registerModules(csvMapper);
        return csvMapper;
    }

    private static void registerModules(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module().configureAbsentsAsNulls(true))
                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private JacksonMapperFactory() {}
}
