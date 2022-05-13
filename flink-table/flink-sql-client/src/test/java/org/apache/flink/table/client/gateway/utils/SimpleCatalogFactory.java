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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.WrappingRuntimeException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Catalog factory for an in-memory catalog that contains a single non-empty table. The contents of
 * the table are equal to {@link SimpleCatalogFactory#TABLE_CONTENTS}.
 */
public class SimpleCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "simple-catalog";

    public static final List<Row> TABLE_CONTENTS =
            Arrays.asList(
                    Row.of(1, "Hello"), Row.of(2, "Hello world"), Row.of(3, "Hello world! Hello!"));

    private static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .defaultValue("default_database");

    private static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("test-table").stringType().defaultValue("test-table");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final Configuration configuration = Configuration.fromMap(context.getOptions());
        final String database = configuration.getString(DEFAULT_DATABASE);
        final String tableName = configuration.getString(TABLE_NAME);

        final GenericInMemoryCatalog genericInMemoryCatalog =
                new GenericInMemoryCatalog(context.getName(), database);
        StreamTableSource<Row> tableSource =
                new StreamTableSource<Row>() {
                    @Override
                    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
                        return execEnv.fromCollection(TABLE_CONTENTS)
                                .returns(
                                        new RowTypeInfo(
                                                new TypeInformation[] {Types.INT(), Types.STRING()},
                                                new String[] {"id", "string"}));
                    }

                    @Override
                    public TableSchema getTableSchema() {
                        return TableSchema.builder()
                                .field("id", DataTypes.INT())
                                .field("string", DataTypes.STRING())
                                .build();
                    }

                    @Override
                    public DataType getProducedDataType() {
                        return DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("string", DataTypes.STRING()))
                                .notNull();
                    }
                };

        try {
            genericInMemoryCatalog.createTable(
                    new ObjectPath(database, tableName),
                    ConnectorCatalogTable.source(tableSource, false),
                    false);
        } catch (Exception e) {
            throw new WrappingRuntimeException(e);
        }

        return genericInMemoryCatalog;
    }
}
