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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;

import org.junit.jupiter.api.Test;

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
}
