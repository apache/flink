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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/** Tests for {@link DynamicTableSinkSpec} serialization and deserialization. */
@RunWith(Parameterized.class)
public class DynamicTableSinkSpecSerdeTest {

    @Parameterized.Parameter public DynamicTableSinkSpec spec;

    @Test
    public void testDynamicTableSinkSpecSerde() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        SerdeContext serdeCtx = new SerdeContext(new Configuration(), classLoader);
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(
                DynamicTableSinkSpec.class, new DynamicTableSinkSpecJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(spec);
        }
        String json = writer.toString();
        DynamicTableSinkSpec actual = mapper.readValue(json, DynamicTableSinkSpec.class);
        assertEquals(spec, actual);
        assertSame(classLoader, actual.getClassLoader());
        assertNotNull(actual.getTableSink());
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<DynamicTableSinkSpec> testData() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "filesystem");
        properties.put("format", "testcsv");
        properties.put("path", "/tmp");
        properties.put("schema.0.name", "a");
        properties.put("schema.0.data-type", "BIGINT");

        DynamicTableSinkSpec spec =
                new DynamicTableSinkSpec(
                        ObjectIdentifier.of("default_catalog", "default_db", "MyTable"),
                        CatalogTableImpl.fromProperties(properties));
        spec.setReadableConfig(new Configuration());

        return Collections.singletonList(spec);
    }
}
