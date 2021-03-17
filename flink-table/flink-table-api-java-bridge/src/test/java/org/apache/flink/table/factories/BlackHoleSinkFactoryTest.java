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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Tests for {@link BlackHoleTableSinkFactory}. */
public class BlackHoleSinkFactoryTest {

    private static final TableSchema TEST_SCHEMA =
            TableSchema.builder()
                    .field("f0", DataTypes.STRING())
                    .field("f1", DataTypes.BIGINT())
                    .field("f2", DataTypes.BIGINT())
                    .build();

    @Test
    public void testBlackHole() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "blackhole");

        DynamicTableSink sink = createSink(properties);

        assertEquals("BlackHole", sink.asSummaryString());
    }

    private DynamicTableSink createSink(Map<String, String> properties) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("", "", ""),
                new CatalogTableImpl(TEST_SCHEMA, properties, ""),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    @Test
    public void testWrongKey() {
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put("connector", "blackhole");
            properties.put("unknown-key", "1");
            createSink(properties);
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage().contains("Unsupported options:\n\nunknown-key"));
            return;
        }
        Assert.fail("Should fail by ValidationException.");
    }
}
