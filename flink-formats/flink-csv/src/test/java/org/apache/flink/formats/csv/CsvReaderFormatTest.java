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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.io.InputStreamFSInputWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CsvReaderFormatTest {

    @Test
    void testForPojoSerializability() throws IOException, ClassNotFoundException {
        final CsvReaderFormat<Pojo> format = CsvReaderFormat.forPojo(Pojo.class);

        final byte[] bytes = InstantiationUtil.serializeObject(format);
        InstantiationUtil.deserializeObject(bytes, CsvReaderFormatTest.class.getClassLoader());
    }

    @Test
    void testForSchemaSerializability() throws IOException, ClassNotFoundException {
        final CsvSchema schema = new CsvMapper().schemaFor(Pojo.class);
        final CsvReaderFormat<Pojo> format =
                CsvReaderFormat.forSchema(schema, TypeInformation.of(Pojo.class));

        final byte[] bytes = InstantiationUtil.serializeObject(format);
        InstantiationUtil.deserializeObject(bytes, CsvReaderFormatTest.class.getClassLoader());
    }

    @Test
    void testForSchemaWithMapperSerializability() throws IOException, ClassNotFoundException {
        final CsvReaderFormat<Pojo> format =
                CsvReaderFormat.forSchema(
                        () -> new CsvMapper(),
                        mapper -> mapper.schemaFor(Pojo.class),
                        TypeInformation.of(Pojo.class));

        final byte[] bytes = InstantiationUtil.serializeObject(format);
        InstantiationUtil.deserializeObject(bytes, CsvReaderFormatTest.class.getClassLoader());
    }

    /**
     * Verifies that we don't use eagerly use the mapper factory in the constructor to initialize
     * some non-transient field.
     */
    @Test
    void testForSchemaWithMapperSerializabilityWithUnserializableMapper()
            throws IOException, ClassNotFoundException {
        final CsvReaderFormat<Pojo> format =
                CsvReaderFormat.forSchema(
                        () -> {
                            final CsvMapper csvMapper = new CsvMapper();
                            // this module is not serializable
                            csvMapper.registerModule(new JavaTimeModule());
                            return csvMapper;
                        },
                        mapper -> mapper.schemaFor(Pojo.class),
                        TypeInformation.of(Pojo.class));

        final byte[] bytes = InstantiationUtil.serializeObject(format);
        InstantiationUtil.deserializeObject(bytes, CsvReaderFormatTest.class.getClassLoader());
    }

    @Test
    void testCreatedMapperPassedToSchemaFunction() throws IOException, ClassNotFoundException {
        final CsvMapper csvMapper = new CsvMapper();

        AtomicReference<CsvMapper> passedMapper = new AtomicReference<>();

        final CsvReaderFormat<Pojo> format =
                CsvReaderFormat.forSchema(
                        () -> csvMapper,
                        mapper -> {
                            passedMapper.set(csvMapper);
                            return mapper.schemaFor(Pojo.class);
                        },
                        TypeInformation.of(Pojo.class));

        format.createReader(
                new Configuration(),
                new InputStreamFSInputWrapper(new ByteArrayInputStream(new byte[0])));
        assertThat(passedMapper.get()).isSameAs(csvMapper);
    }

    public static class Pojo {
        public int x;
    }
}
