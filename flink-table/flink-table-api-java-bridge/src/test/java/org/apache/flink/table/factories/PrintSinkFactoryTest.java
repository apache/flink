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
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.PrintTableSinkFactory.PRINT_IDENTIFIER;
import static org.apache.flink.table.factories.PrintTableSinkFactory.STANDARD_ERROR;

/** Tests for {@link PrintTableSinkFactory}. */
public class PrintSinkFactoryTest {

    private static final TableSchema TEST_SCHEMA =
            TableSchema.builder()
                    .field("f0", DataTypes.STRING())
                    .field("f1", DataTypes.BIGINT())
                    .field("f2", DataTypes.BIGINT())
                    .build();

    @Test
    public void testPrint() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "print");
        properties.put(PRINT_IDENTIFIER.key(), "my_print");
        properties.put(STANDARD_ERROR.key(), "true");

        DynamicTableSink sink =
                FactoryUtil.createTableSink(
                        null,
                        ObjectIdentifier.of("", "", ""),
                        new CatalogTableImpl(TEST_SCHEMA, properties, ""),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        Assert.assertEquals("Print to System.err", sink.asSummaryString());
    }
}
