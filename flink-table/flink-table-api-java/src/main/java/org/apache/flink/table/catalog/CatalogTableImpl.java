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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * A catalog table implementation.
 *
 * @deprecated Use {@link CatalogTable#of(Schema, String, List, Map)} or a custom implementation
 *     instead. Don't implement against this internal class. It can lead to unintended side effects
 *     if code checks against this class instead of the common interface.
 */
@Deprecated
@Internal
public class CatalogTableImpl extends AbstractCatalogTable {

    public CatalogTableImpl(
            TableSchema tableSchema, Map<String, String> properties, String comment) {
        this(tableSchema, new ArrayList<>(), properties, comment);
    }

    public CatalogTableImpl(
            TableSchema tableSchema,
            List<String> partitionKeys,
            Map<String, String> properties,
            String comment) {
        super(tableSchema, partitionKeys, properties, comment);
    }

    @Override
    public CatalogBaseTable copy() {
        return new CatalogTableImpl(
                getSchema().copy(),
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(getOptions()),
                getComment());
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of("This is a catalog table in an im-memory catalog");
    }

    @Override
    public Map<String, String> toProperties() {
        DescriptorProperties descriptor = new DescriptorProperties(false);

        descriptor.putTableSchema(SCHEMA, getSchema());
        descriptor.putPartitionKeys(getPartitionKeys());

        Map<String, String> properties = new HashMap<>(getOptions());

        descriptor.putProperties(properties);

        return descriptor.asMap();
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new CatalogTableImpl(getSchema(), getPartitionKeys(), options, getComment());
    }

    /** Construct a {@link CatalogTableImpl} from complete properties that contains table schema. */
    public static CatalogTableImpl fromProperties(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(false);
        descriptorProperties.putProperties(properties);
        TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
        List<String> partitionKeys = descriptorProperties.getPartitionKeys();
        return new CatalogTableImpl(
                tableSchema,
                partitionKeys,
                removeRedundant(properties, tableSchema, partitionKeys),
                "");
    }

    /** Construct catalog table properties from {@link #toProperties()}. */
    public static Map<String, String> removeRedundant(
            Map<String, String> properties, TableSchema schema, List<String> partitionKeys) {
        Map<String, String> ret = new HashMap<>(properties);
        DescriptorProperties descriptorProperties = new DescriptorProperties(false);
        descriptorProperties.putTableSchema(SCHEMA, schema);
        descriptorProperties.putPartitionKeys(partitionKeys);
        descriptorProperties.asMap().keySet().forEach(ret::remove);
        return ret;
    }
}
