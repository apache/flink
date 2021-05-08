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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A utility class for mocking {@link DynamicTableFactory.Context}. */
class TestContext {

    private ResolvedSchema schema = ResolvedSchema.of(Column.physical("a", DataTypes.TIME()));

    private final Map<String, String> options = new HashMap<>();

    public static TestContext context() {
        return new TestContext();
    }

    public TestContext withSchema(ResolvedSchema schema) {
        this.schema = schema;
        return this;
    }

    DynamicTableFactory.Context build() {
        return new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock context",
                                Collections.emptyList(),
                                options),
                        schema),
                new Configuration(),
                TestContext.class.getClassLoader(),
                false);
    }

    public TestContext withOption(String key, String value) {
        options.put(key, value);
        return this;
    }
}
