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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.planner.runtime.utils.TestingAppendTableSink;
import org.apache.flink.table.planner.utils.TestTableSource;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/** A legacy {@link TableFactory} uses user define options. */
public class LegacyTableFactory
        implements StreamTableSinkFactory<Row>, StreamTableSourceFactory<Row> {

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties dp = new DescriptorProperties();
        dp.putProperties(properties);
        TableSchema tableSchema = dp.getTableSchema(SCHEMA);
        StreamTableSink<Row> sink = new TestingAppendTableSink();
        return (StreamTableSink)
                sink.configure(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties dp = new DescriptorProperties();
        dp.putProperties(properties);
        TableSchema tableSchema = dp.getTableSchema(SCHEMA);
        return new TestTableSource(false, tableSchema);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> options = new HashMap<>();
        options.put("type", "legacy");
        return options;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // schema
        properties.add(SCHEMA + ".#." + DescriptorProperties.TYPE);
        properties.add(SCHEMA + ".#." + DescriptorProperties.DATA_TYPE);
        properties.add(SCHEMA + ".#." + DescriptorProperties.NAME);
        properties.add(SCHEMA + ".#." + DescriptorProperties.EXPR);
        return properties;
    }
}
