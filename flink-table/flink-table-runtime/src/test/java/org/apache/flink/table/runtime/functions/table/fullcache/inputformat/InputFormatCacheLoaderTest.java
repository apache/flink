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

package org.apache.flink.table.runtime.functions.table.fullcache.inputformat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.cache.InterceptingCacheMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.functions.table.fullcache.TestCacheLoader;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.inputformat.InputFormatCacheLoader;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.metrics.groups.InternalCacheMetricGroup.UNINITIALIZED;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link InputFormatCacheLoader}. */
class InputFormatCacheLoaderTest {

    @ParameterizedTest
    @MethodSource("deltaNumSplits")
    void testReadWithDifferentSplits(int deltaNumSplits) throws Exception {
        InputFormatCacheLoader cacheLoader = createCacheLoader(deltaNumSplits, null);
        cacheLoader.open(UnregisteredMetricsGroup.createCacheMetricGroup());
        cacheLoader.run();
        ConcurrentHashMap<RowData, Collection<RowData>> cache = cacheLoader.getCache();
        assertCacheContent(cache);
        cacheLoader.run();
        assertThat(cacheLoader.getCache()).isNotSameAs(cache); // new instance of cache after reload
        cacheLoader.close();
        assertThat(cacheLoader.getCache().size()).isZero(); // cache is cleared after close
    }

    @Test
    void testCacheMetrics() throws Exception {
        InputFormatCacheLoader cacheLoader = createCacheLoader(0, null);
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        cacheLoader.open(metricGroup);
        // These metrics are registered
        assertThat(metricGroup.loadCounter).isNotNull();
        assertThat(metricGroup.loadCounter.getCount()).isEqualTo(0);
        assertThat(metricGroup.numLoadFailuresCounter).isNotNull();
        assertThat(metricGroup.numLoadFailuresCounter.getCount()).isEqualTo(0);
        assertThat(metricGroup.numCachedRecordsGauge).isNotNull();
        assertThat(metricGroup.numCachedRecordsGauge.getValue()).isEqualTo(0);
        assertThat(metricGroup.latestLoadTimeGauge).isNotNull();
        assertThat(metricGroup.latestLoadTimeGauge.getValue()).isEqualTo(UNINITIALIZED);

        // These metrics are left blank
        assertThat(metricGroup.hitCounter).isNull();
        assertThat(metricGroup.missCounter).isNull();
        assertThat(metricGroup.numCachedBytesGauge).isNull(); // not supported currently

        cacheLoader.run();

        assertThat(metricGroup.loadCounter.getCount()).isEqualTo(1);
        assertThat(metricGroup.latestLoadTimeGauge.getValue()).isNotEqualTo(UNINITIALIZED);
        assertThat(metricGroup.numCachedRecordsGauge.getValue())
                .isEqualTo(TestCacheLoader.DATA.size());
    }

    @Test
    void testExceptionHandling() throws Exception {
        RuntimeException exception = new RuntimeException("Load failed.");
        InputFormatCacheLoader cacheLoader = createCacheLoader(0, exception);
        InterceptingCacheMetricGroup metricGroup = new InterceptingCacheMetricGroup();
        cacheLoader.open(metricGroup);
        assertThatThrownBy(cacheLoader::run).hasRootCause(exception);
        assertThat(metricGroup.numLoadFailuresCounter.getCount()).isEqualTo(1);
    }

    static Stream<Arguments> deltaNumSplits() {
        return Stream.of(Arguments.of(-1), Arguments.of(0), Arguments.of(1));
    }

    // ----------------------- Helper functions ----------------------

    private void assertCacheContent(Map<RowData, Collection<RowData>> actual) {
        assertThat(actual).containsOnlyKeys(TestCacheLoader.DATA.keySet());
        TestCacheLoader.DATA.forEach(
                (key, rows) ->
                        assertThat(rows).containsExactlyInAnyOrderElementsOf(actual.get(key)));
    }

    private InputFormatCacheLoader createCacheLoader(
            int deltaNumSplits, RuntimeException testException) throws Exception {
        DataType rightRowDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("f0", DataTypes.INT()),
                        DataTypes.FIELD("f1", DataTypes.STRING().bridgedTo(String.class)));

        RowDataSerializer rightRowSerializer =
                (RowDataSerializer)
                        InternalSerializers.<RowData>create(rightRowDataType.getLogicalType());
        DataType[] dataTypes = rightRowDataType.getChildren().toArray(new DataType[] {});
        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(dataTypes);

        Collection<Row> dataRows =
                TestCacheLoader.DATA.values().stream()
                        .map(Collection::stream)
                        .reduce(Stream.empty(), Stream::concat)
                        .map(converter::toExternal)
                        .collect(Collectors.toList());
        FullCacheTestInputFormat inputFormat =
                new FullCacheTestInputFormat(dataRows, Optional.empty(), converter, deltaNumSplits);
        RowType keyType = (RowType) DataTypes.ROW(DataTypes.INT()).getLogicalType();

        // noinspection rawtypes
        GeneratedProjection generatedProjection =
                new GeneratedProjection("", "", new Object[0]) {
                    @Override
                    public Projection newInstance(ClassLoader classLoader) {
                        if (testException != null) {
                            throw testException;
                        }
                        return row -> row(row.getInt(0));
                    }
                };
        GenericRowDataKeySelector keySelector =
                new GenericRowDataKeySelector(
                        InternalTypeInfo.of(keyType),
                        InternalSerializers.create(keyType),
                        generatedProjection);
        InputFormatCacheLoader cacheLoader =
                new InputFormatCacheLoader(inputFormat, keySelector, rightRowSerializer);
        cacheLoader.open(new Configuration());
        return cacheLoader;
    }
}
