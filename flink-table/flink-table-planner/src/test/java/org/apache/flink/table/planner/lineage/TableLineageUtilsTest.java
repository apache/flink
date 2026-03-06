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

package org.apache.flink.table.planner.lineage;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.DefaultLineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.planner.lineage.TableLineageUtils.createTableLineageDataset;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableLineageUtils}. */
class TableLineageUtilsTest {

    private static final Catalog CATALOG = new GenericInMemoryCatalog(DEFAULT_CATALOG, "db1");
    private static final ResolvedSchema CATALOG_TABLE_RESOLVED_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("a", DataTypes.STRING()),
                            Column.physical("b", DataTypes.INT()),
                            Column.physical("c", DataTypes.BOOLEAN())),
                    Collections.emptyList(),
                    null,
                    Collections.emptyList());
    private static final Schema CATALOG_TABLE_SCHEMA =
            Schema.newBuilder().fromResolvedSchema(CATALOG_TABLE_RESOLVED_SCHEMA).build();
    private static final Map<String, String> TABLE_OPTIONS = new HashMap<>();

    static {
        TABLE_OPTIONS.put("a", "1");
        TABLE_OPTIONS.put("b", "10");
        TABLE_OPTIONS.put("d", "4");
    }

    @Test
    void testCreateTableLineageDatasetWithCatalog() {
        final ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of(DEFAULT_CATALOG, "my_db", "my_permanent_table");
        final ContextResolvedTable resolvedTable =
                ContextResolvedTable.permanent(
                        objectIdentifier,
                        CATALOG,
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(CATALOG_TABLE_SCHEMA)
                                        .comment("my table")
                                        .partitionKeys(Collections.emptyList())
                                        .options(TABLE_OPTIONS)
                                        .build(),
                                CATALOG_TABLE_RESOLVED_SCHEMA));

        LineageDataset lineageDataset = createTableLineageDataset(resolvedTable, Optional.empty());
        assertThat(lineageDataset).isInstanceOf(TableLineageDatasetImpl.class);

        TableLineageDatasetImpl tableLineageDataset = (TableLineageDatasetImpl) lineageDataset;
        assertThat(tableLineageDataset.catalogContext().getCatalogName())
                .isEqualTo(DEFAULT_CATALOG);
        assertThat(tableLineageDataset.name()).isEqualTo(objectIdentifier.asSummaryString());
    }

    @Test
    void testCreateTableLineageDatasetWithoutCatalog() {
        final ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("default_cat", "default_db", "my_temporary_table");
        final ContextResolvedTable resolvedTable =
                ContextResolvedTable.temporary(
                        objectIdentifier,
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(CATALOG_TABLE_SCHEMA)
                                        .comment("my table")
                                        .partitionKeys(Collections.emptyList())
                                        .options(TABLE_OPTIONS)
                                        .build(),
                                CATALOG_TABLE_RESOLVED_SCHEMA));

        LineageDataset lineageDataset = createTableLineageDataset(resolvedTable, Optional.empty());
        assertThat(lineageDataset).isInstanceOf(TableLineageDatasetImpl.class);

        TableLineageDatasetImpl tableLineageDataset = (TableLineageDatasetImpl) lineageDataset;
        assertThat(tableLineageDataset.catalogContext().getCatalogName()).isEmpty();
        assertThat(tableLineageDataset.name()).isEqualTo(objectIdentifier.asSummaryString());
    }

    @Test
    void testExtractLineageDatasetFromDataStreamScanProvider() {
        DataStreamScanProvider provider =
                new DataStreamScanProvider() {
                    @Override
                    public DataStream<RowData> produceDataStream(
                            ProviderContext providerContext,
                            StreamExecutionEnvironment execEnv) {
                        return null;
                    }

                    @Override
                    public boolean isBounded() {
                        return false;
                    }
                };

        // A plain DataStreamScanProvider without LineageVertexProvider should return empty
        Optional<LineageVertex> result = TableLineageUtils.extractLineageDataset(provider);
        assertThat(result).isEmpty();
    }

    @Test
    void testExtractLineageDatasetFromDataStreamScanProviderWithLineage() {
        DataStreamScanProvider provider =
                new TestDataStreamScanProviderWithLineage("test-source", "test://namespace");

        Optional<LineageVertex> result = TableLineageUtils.extractLineageDataset(provider);
        assertThat(result).isPresent();
        assertThat(result.get().datasets()).hasSize(1);
        assertThat(result.get().datasets().get(0).name()).isEqualTo("test-source");
        assertThat(result.get().datasets().get(0).namespace()).isEqualTo("test://namespace");
    }

    @Test
    void testExtractLineageDatasetFromDataStreamSinkProvider() {
        DataStreamSinkProvider provider =
                new DataStreamSinkProvider() {
                    @Override
                    public DataStreamSink<?> consumeDataStream(
                            ProviderContext providerContext,
                            DataStream<RowData> dataStream) {
                        return null;
                    }
                };

        // A plain DataStreamSinkProvider without LineageVertexProvider should return empty
        Optional<LineageVertex> result = TableLineageUtils.extractLineageDataset(provider);
        assertThat(result).isEmpty();
    }

    @Test
    void testExtractLineageDatasetFromDataStreamSinkProviderWithLineage() {
        DataStreamSinkProvider provider =
                new TestDataStreamSinkProviderWithLineage("test-sink", "test://namespace");

        Optional<LineageVertex> result = TableLineageUtils.extractLineageDataset(provider);
        assertThat(result).isPresent();
        assertThat(result.get().datasets()).hasSize(1);
        assertThat(result.get().datasets().get(0).name()).isEqualTo("test-sink");
        assertThat(result.get().datasets().get(0).namespace()).isEqualTo("test://namespace");
    }

    /**
     * A {@link DataStreamScanProvider} that also implements {@link LineageVertexProvider}, similar
     * to how Apache Paimon's PaimonDataStreamScanProvider works.
     */
    private static class TestDataStreamScanProviderWithLineage
            implements DataStreamScanProvider, LineageVertexProvider {
        private final String name;
        private final String namespace;

        TestDataStreamScanProviderWithLineage(String name, String namespace) {
            this.name = name;
            this.namespace = namespace;
        }

        @Override
        public DataStream<RowData> produceDataStream(
                ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
            return null;
        }

        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public LineageVertex getLineageVertex() {
            return () ->
                    List.of(new DefaultLineageDataset(name, namespace, Collections.emptyMap()));
        }
    }

    /**
     * A {@link DataStreamSinkProvider} that also implements {@link LineageVertexProvider}, similar
     * to how Apache Paimon's PaimonDataStreamSinkProvider works.
     */
    private static class TestDataStreamSinkProviderWithLineage
            implements DataStreamSinkProvider, LineageVertexProvider {
        private final String name;
        private final String namespace;

        TestDataStreamSinkProviderWithLineage(String name, String namespace) {
            this.name = name;
            this.namespace = namespace;
        }

        @Override
        public DataStreamSink<?> consumeDataStream(
                ProviderContext providerContext, DataStream<RowData> dataStream) {
            return null;
        }

        @Override
        public LineageVertex getLineageVertex() {
            return () ->
                    List.of(new DefaultLineageDataset(name, namespace, Collections.emptyMap()));
        }
    }
}
