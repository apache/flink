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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters.LocalDateConverter;
import org.apache.flink.table.data.util.DataFormatConverters.LocalDateTimeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for HiveSourceDynamicFileEnumerator. */
class HiveSourceDynamicFileEnumeratorTest {

    @Test
    void testFiltering() {
        List<String> keys = Collections.singletonList("a");
        List<Map<String, String>> partitionSpecs =
                Arrays.asList(
                        Collections.singletonMap("a", "31"), Collections.singletonMap("a", "32"));
        HiveSourceDynamicFileEnumerator enumerator = createTestEnumerator(keys, partitionSpecs);

        assertThat(enumerator.getFinalPartitions()).hasSize(2);
        assertThat(
                        enumerator.getFinalPartitions().stream()
                                .map(HiveTablePartition::getPartitionSpec)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(partitionSpecs.toArray(new Map[0]));

        RowType rowType = RowType.of(new IntType());
        TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
        GenericRowData filteringRow = new GenericRowData(1);
        filteringRow.setField(0, 31);
        DynamicFilteringData data =
                new DynamicFilteringData(
                        InternalTypeInfo.of(rowType),
                        rowType,
                        Collections.singletonList(serialize(rowTypeInfo, filteringRow)),
                        true);
        enumerator.setDynamicFilteringData(data);

        assertThat(enumerator.getFinalPartitions()).hasSize(1);
        assertThat(enumerator.getFinalPartitions().get(0).getPartitionSpec().get("a"))
                .isEqualTo("31");
    }

    @Test
    void testCreateRowSupportedTypes() {
        List<Tuple3<LogicalType, Object, String>> testTypeValues = new ArrayList<>();
        testTypeValues.add(new Tuple3<>(new IntType(), 42, "42"));
        testTypeValues.add(new Tuple3<>(new BigIntType(), 9876543210L, "9876543210"));
        testTypeValues.add(new Tuple3<>(new SmallIntType(), (short) 41, "41"));
        testTypeValues.add(new Tuple3<>(new TinyIntType(), (byte) 40, "40"));
        testTypeValues.add(new Tuple3<>(new VarCharType(), StringData.fromString("1234"), "1234"));
        testTypeValues.add(new Tuple3<>(new CharType(), StringData.fromString("7"), "7"));
        testTypeValues.add(
                new Tuple3<>(
                        new DateType(),
                        LocalDateConverter.INSTANCE.toInternal(LocalDate.of(2022, 2, 22)),
                        "2022-2-22"));
        testTypeValues.add(
                new Tuple3<>(
                        new TimestampType(9),
                        new LocalDateTimeConverter(9)
                                .toInternal(LocalDateTime.of(2022, 2, 22, 22, 2, 20, 20222022)),
                        "2022-2-22 22:02:20.020222022"));

        // Default partition values
        testTypeValues.add(
                new Tuple3<>(
                        new VarCharType(),
                        StringData.fromString(HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal),
                        HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal));
        testTypeValues.add(
                new Tuple3<>(
                        new IntType(), null, HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal));

        RowType rowType =
                RowType.of(testTypeValues.stream().map(t -> t.f0).toArray(LogicalType[]::new));
        List<String> keys = new ArrayList<>();
        Map<String, String> spec = new HashMap<>();
        for (int i = 0; i < testTypeValues.size(); i++) {
            keys.add(String.valueOf(i));
            spec.put(String.valueOf(i), testTypeValues.get(i).f2);
        }
        HiveSourceDynamicFileEnumerator enumerator =
                createTestEnumerator(keys, Collections.emptyList());

        RowData result = enumerator.createRowData(rowType, spec);

        for (int i = 0; i < testTypeValues.size(); i++) {
            LogicalType type = testTypeValues.get(i).f0;
            Object expected = testTypeValues.get(i).f1;
            FieldGetter getter = RowData.createFieldGetter(type, i);
            assertThat(getter.getFieldOrNull(result))
                    .withFailMessage(
                            () ->
                                    "Mismatching row type "
                                            + type
                                            + ", expected:"
                                            + expected
                                            + ", actual:"
                                            + getter.getFieldOrNull(result))
                    .isEqualTo(testTypeValues.get(i).f1);
        }
    }

    @Test
    void testNonNullFieldTypeWithDefaultPartitionName() {
        String defaultPartitionName = HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal;
        List<String> keys = Arrays.asList("NonNullString", "NonNullInt");
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("NonNullString", defaultPartitionName);
        partitionSpec.put("NonNullInt", "0");

        Map<String, String> partitionSpec2 = new HashMap<>();
        partitionSpec2.put("NonNullString", "");
        partitionSpec2.put("NonNullInt", defaultPartitionName);

        List<Map<String, String>> partitionSpecs = Arrays.asList(partitionSpec, partitionSpec2);
        HiveSourceDynamicFileEnumerator enumerator = createTestEnumerator(keys, partitionSpecs);

        assertThat(enumerator.getFinalPartitions()).hasSize(2);
        assertThat(
                        enumerator.getFinalPartitions().stream()
                                .map(HiveTablePartition::getPartitionSpec)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(partitionSpecs.toArray(new Map[0]));

        RowType rowType = RowType.of(new VarCharType(false, 32), new IntType(false));
        TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
        DynamicFilteringData data =
                new DynamicFilteringData(
                        InternalTypeInfo.of(rowType),
                        rowType,
                        Arrays.asList(
                                serialize(
                                        rowTypeInfo,
                                        GenericRowData.of(fromString(defaultPartitionName), 0)),
                                serialize(rowTypeInfo, GenericRowData.of(fromString(""), 0))),
                        true);
        enumerator.setDynamicFilteringData(data);

        // No exception should be thrown and partitionSpec2 should not be retained
        assertThat(enumerator.getFinalPartitions()).hasSize(1);
        assertThat(enumerator.getFinalPartitions().get(0).getPartitionSpec())
                .isEqualTo(partitionSpec);
    }

    private HiveSourceDynamicFileEnumerator createTestEnumerator(
            List<String> keys, List<Map<String, String>> partitionSpecs) {
        return new HiveSourceDynamicFileEnumerator(
                "",
                keys,
                partitionSpecs.stream()
                        .map(
                                spec ->
                                        new HiveTablePartition(
                                                new StorageDescriptor(), spec, new Properties()))
                        .collect(Collectors.toList()),
                HiveShimLoader.getHiveVersion(),
                new JobConf());
    }

    private byte[] serialize(TypeInformation<RowData> typeInfo, RowData row) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            typeInfo.createSerializer(new ExecutionConfig())
                    .serialize(row, new DataOutputViewStreamWrapper(baos));
        } catch (IOException e) {
            // throw as RuntimeException so the function can use in lambda
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }
}
