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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvParser;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class JacksonMapperFactoryTest {

    @Test
    void testCreateObjectMapperReturnDistinctMappers() {
        final ObjectMapper mapper1 = JacksonMapperFactory.createObjectMapper();
        final ObjectMapper mapper2 = JacksonMapperFactory.createObjectMapper();

        assertThat(mapper1).isNotSameAs(mapper2);
    }

    @Test
    void testCreateCsvMapperReturnDistinctMappers() {
        final CsvMapper mapper1 = JacksonMapperFactory.createCsvMapper();
        final CsvMapper mapper2 = JacksonMapperFactory.createCsvMapper();

        assertThat(mapper1).isNotSameAs(mapper2);
    }

    @Test
    void testObjectMapperOptionalSupportedEnabled() throws Exception {
        final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

        assertThat(mapper.writeValueAsString(new TypeWithOptional(Optional.of("value"))))
                .isEqualTo("{\"data\":\"value\"}");
        assertThat(mapper.writeValueAsString(new TypeWithOptional(Optional.empty())))
                .isEqualTo("{\"data\":null}");

        assertThat(mapper.readValue("{\"data\":\"value\"}", TypeWithOptional.class).data)
                .contains("value");
        assertThat(mapper.readValue("{\"data\":null}", TypeWithOptional.class).data).isEmpty();
        assertThat(mapper.readValue("{}", TypeWithOptional.class).data).isEmpty();
    }

    @Test
    void testCsvMapperOptionalSupportedEnabled() throws Exception {
        final CsvMapper mapper =
                JacksonMapperFactory.createCsvMapper()
                        // ensures symmetric read/write behavior for empty optionals/strings
                        // ensures:   Optional.empty() --write--> "" --read--> Optional.empty()
                        // otherwise: Optional.empty() --write--> "" --read--> Optional("")
                        // we should consider enabling this by default, but it unfortunately
                        // also affects String parsing without Optionals (i.e., prior code)
                        .enable(CsvParser.Feature.EMPTY_STRING_AS_NULL);

        final ObjectWriter writer = mapper.writerWithSchemaFor(TypeWithOptional.class);

        assertThat(writer.writeValueAsString(new TypeWithOptional(Optional.of("value"))))
                .isEqualTo("value\n");
        assertThat(writer.writeValueAsString(new TypeWithOptional(Optional.empty())))
                .isEqualTo("\n");

        final ObjectReader reader = mapper.readerWithSchemaFor(TypeWithOptional.class);

        assertThat(reader.readValue("value\n", TypeWithOptional.class).data).contains("value");
        assertThat(reader.readValue("null\n", TypeWithOptional.class).data).contains("null");
        assertThat(reader.readValue("\n", TypeWithOptional.class).data).isEmpty();
    }

    @Test
    void testObjectMappeDateTimeSupportedEnabled() throws Exception {
        final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

        final String instantString = "2022-08-07T12:00:33.107787800Z";
        final Instant instant = Instant.parse(instantString);
        final String instantJson = String.format("{\"data\":\"%s\"}", instantString);

        assertThat(mapper.writeValueAsString(new TypeWithInstant(instant))).isEqualTo(instantJson);
        assertThat(mapper.readValue(instantJson, TypeWithInstant.class).data).isEqualTo(instant);
    }

    @Test
    void testCsvMapperDateTimeSupportedEnabled() throws Exception {
        final CsvMapper mapper = JacksonMapperFactory.createCsvMapper();

        final String instantString = "2022-08-07T12:00:33.107787800Z";
        final Instant instant = Instant.parse(instantString);
        final String instantCsv = String.format("\"%s\"\n", instantString);

        final ObjectWriter writer = mapper.writerWithSchemaFor(TypeWithInstant.class);

        assertThat(writer.writeValueAsString(new TypeWithInstant(instant))).isEqualTo(instantCsv);

        final ObjectReader reader = mapper.readerWithSchemaFor(TypeWithInstant.class);

        assertThat(reader.readValue(instantCsv, TypeWithInstant.class).data).isEqualTo(instant);
    }

    public static class TypeWithOptional {
        public Optional<String> data;

        @JsonCreator
        public TypeWithOptional(@JsonProperty("data") Optional<String> data) {
            this.data = data;
        }
    }

    public static class TypeWithInstant {
        public Instant data;

        @JsonCreator
        public TypeWithInstant(@JsonProperty("data") Instant data) {
            this.data = data;
        }
    }
}
