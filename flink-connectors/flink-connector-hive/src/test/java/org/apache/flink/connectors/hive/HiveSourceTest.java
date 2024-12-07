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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.DynamicFilteringInfo;
import org.apache.flink.api.connector.source.DynamicParallelismInference;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HiveSource}. */
class HiveSourceTest {

    private static HiveCatalog hiveCatalog;

    private static final List<String> keys = Collections.singletonList("p");

    private static final List<Map<String, String>> partitionSpecs =
            Arrays.asList(
                    Collections.singletonMap("p", "1"),
                    Collections.singletonMap("p", "2"),
                    Collections.singletonMap("p", "3"));

    @BeforeAll
    static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterAll
    static void tearDown() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    void testDynamicParallelismInference() throws Exception {
        // test non-partitioned table
        ObjectPath tablePath1 = new ObjectPath("default", "hiveNonPartTbl");
        createTable(tablePath1, hiveCatalog, false);

        HiveSource<RowData> hiveSource =
                new HiveSourceBuilder(
                                new JobConf(hiveCatalog.getHiveConf()),
                                new Configuration(),
                                HiveShimLoader.getHiveVersion(),
                                tablePath1.getDatabaseName(),
                                tablePath1.getObjectName(),
                                Collections.emptyMap())
                        .buildWithDefaultBulkFormat();

        DynamicParallelismInference.Context context =
                genDynamicParallelismContext(10, Collections.emptyList());
        assertThat(hiveSource.inferParallelism(context)).isEqualTo(1);

        hiveCatalog.dropTable(tablePath1, false);

        // test partitioned table
        ObjectPath tablePath2 = new ObjectPath("default", "hiveTbl1");
        createTable(tablePath2, hiveCatalog, true);

        hiveSource = createHiveSourceWithPartition(tablePath2, new Configuration(), -1, null);

        // test inferred parallelism less than maxParallelism
        context = genDynamicParallelismContext(10, Collections.emptyList());
        assertThat(hiveSource.inferParallelism(context)).isEqualTo(3);

        // test inferred parallelism larger than maxParallelism
        context = genDynamicParallelismContext(2, Collections.emptyList());
        assertThat(hiveSource.inferParallelism(context)).isEqualTo(2);

        hiveCatalog.dropTable(tablePath2, false);
    }

    @Test
    void testDynamicParallelismInferenceWithLimit() throws Exception {
        ObjectPath tablePath = new ObjectPath("default", "hiveTbl2");
        createTable(tablePath, hiveCatalog, true);

        HiveSource<RowData> hiveSource =
                createHiveSourceWithPartition(tablePath, new Configuration(), 1L, null);

        // test inferred parallelism less than maxParallelism
        DynamicParallelismInference.Context context =
                genDynamicParallelismContext(10, Collections.emptyList());
        assertThat(hiveSource.inferParallelism(context)).isEqualTo(1);

        hiveCatalog.dropTable(tablePath, false);
    }

    @Test
    void testDynamicParallelismInferenceWithFiltering() throws Exception {
        ObjectPath tablePath = new ObjectPath("default", "hiveTbl3");
        createTable(tablePath, hiveCatalog, true);

        HiveSource<RowData> hiveSource =
                createHiveSourceWithPartition(tablePath, new Configuration(), -1, keys);

        DynamicParallelismInference.Context context =
                genDynamicParallelismContext(10, Arrays.asList(1, 2));

        assertThat(hiveSource.inferParallelism(context)).isEqualTo(2);
        hiveCatalog.dropTable(tablePath, false);
    }

    @Test
    void testDynamicParallelismInferenceWithSettingMaxParallelism() throws Exception {
        ObjectPath tablePath = new ObjectPath("default", "hiveTbl4");
        createTable(tablePath, hiveCatalog, true);

        Configuration configuration = new Configuration();
        configuration.set(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX, 1);
        HiveSource<RowData> hiveSource =
                createHiveSourceWithPartition(tablePath, configuration, -1, null);

        DynamicParallelismInference.Context context =
                genDynamicParallelismContext(10, Collections.emptyList());
        assertThat(hiveSource.inferParallelism(context)).isEqualTo(1);

        hiveCatalog.dropTable(tablePath, false);
    }

    private HiveSource<RowData> createHiveSourceWithPartition(
            ObjectPath tablePath,
            Configuration config,
            long limit,
            List<String> dynamicFilterPartitionKeys) {
        HiveSourceBuilder hiveSourceBuilder =
                new HiveSourceBuilder(
                                new JobConf(hiveCatalog.getHiveConf()),
                                config,
                                HiveShimLoader.getHiveVersion(),
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                Collections.emptyMap())
                        .setPartitions(
                                partitionSpecs.stream()
                                        .map(
                                                spec ->
                                                        HiveTablePartition.ofPartition(
                                                                hiveCatalog.getHiveConf(),
                                                                hiveCatalog.getHiveVersion(),
                                                                tablePath.getDatabaseName(),
                                                                tablePath.getObjectName(),
                                                                new LinkedHashMap<>(spec)))
                                        .collect(Collectors.toList()));
        if (limit != -1) {
            hiveSourceBuilder.setLimit(limit);
        }

        if (dynamicFilterPartitionKeys != null) {
            hiveSourceBuilder.setDynamicFilterPartitionKeys(dynamicFilterPartitionKeys);
        }

        return hiveSourceBuilder.buildWithDefaultBulkFormat();
    }

    private void createTable(ObjectPath tablePath, HiveCatalog hiveCatalog, boolean isPartitioned)
            throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), IDENTIFIER);

        List<Column> partitionTableColumns = new ArrayList<>();
        partitionTableColumns.add(Column.physical("i", DataTypes.INT()));
        if (isPartitioned) {
            HiveSourceTest.keys.stream()
                    .map(key -> Column.physical(key, DataTypes.STRING()))
                    .forEach(partitionTableColumns::add);
        }
        ResolvedSchema partitionTableRSchema = ResolvedSchema.of(partitionTableColumns);

        hiveCatalog.createTable(
                tablePath,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder()
                                        .fromResolvedSchema(partitionTableRSchema)
                                        .build(),
                                null,
                                isPartitioned ? keys : Collections.emptyList(),
                                tableOptions),
                        partitionTableRSchema),
                false);

        if (isPartitioned) {
            HiveSourceTest.partitionSpecs.forEach(
                    spec -> {
                        try {
                            HiveTestUtils.createTextTableInserter(
                                            hiveCatalog,
                                            tablePath.getDatabaseName(),
                                            tablePath.getObjectName())
                                    .addRow(new Object[] {1})
                                    .addRow(new Object[] {2})
                                    .commit(
                                            spec.keySet().iterator().next()
                                                    + "='"
                                                    + spec.values().iterator().next()
                                                    + "'");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            HiveTestUtils.createTextTableInserter(
                            hiveCatalog, tablePath.getDatabaseName(), tablePath.getObjectName())
                    .addRow(new Object[] {1})
                    .addRow(new Object[] {2})
                    .commit();
        }
    }

    private DynamicParallelismInference.Context genDynamicParallelismContext(
            int maxParallelism, List<Integer> filteringPartitions) {
        return new DynamicParallelismInference.Context() {
            @Override
            public Optional<DynamicFilteringInfo> getDynamicFilteringInfo() {
                RowType rowType = RowType.of(new IntType());
                TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
                if (!filteringPartitions.isEmpty()) {
                    List<byte[]> serializedRows =
                            filteringPartitions.stream()
                                    .map(
                                            key -> {
                                                GenericRowData filteringRow = new GenericRowData(1);
                                                filteringRow.setField(0, key);
                                                return HiveTestUtils.serialize(
                                                        rowTypeInfo, filteringRow);
                                            })
                                    .collect(Collectors.toList());

                    DynamicFilteringData data =
                            new DynamicFilteringData(
                                    InternalTypeInfo.of(rowType), rowType, serializedRows, true);
                    return Optional.of(new DynamicFilteringEvent(data));
                } else {
                    return Optional.empty();
                }
            }

            @Override
            public int getParallelismInferenceUpperBound() {
                return maxParallelism;
            }

            @Override
            public long getDataVolumePerTask() {
                return 10L;
            }
        };
    }
}
