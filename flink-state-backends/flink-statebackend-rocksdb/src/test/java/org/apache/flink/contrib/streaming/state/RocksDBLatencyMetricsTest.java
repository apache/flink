/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.DELETE_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.GET_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.MERGE_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.NEXT_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.PUT_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.SEEK_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBAccessMetric.WRITE_BATCH_LATENCY;
import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder.DB_INSTANCE_DIR_STRING;

/** Tests for {@link RocksDBAccessMetric}. */
public class RocksDBLatencyMetricsTest {
    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private final int sampleInterval = 20;

    private RocksDB db;
    private ColumnFamilyHandle defaultCFHandle;
    private RocksDBResourceContainer optionsContainer;
    private RocksDBKeyedStateBackend<Integer> stateBackend;
    private ValueState<String> valueState;
    private Map<String, Histogram> valueStateHistogram;
    private ListState<String> listState;
    private Map<String, Histogram> listStateHistogram;
    private MapState<Integer, String> mapState;
    private Map<String, Histogram> mapStateHistogram;

    @Before
    public void setupRocksDB() throws Exception {
        optionsContainer = new RocksDBResourceContainer();
        String dbPath = new File(TEMP_FOLDER.newFolder(), DB_INSTANCE_DIR_STRING).getAbsolutePath();
        ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();

        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        db =
                RocksDBOperationUtils.openDB(
                        dbPath,
                        Collections.emptyList(),
                        columnFamilyHandles,
                        columnOptions,
                        optionsContainer.getDbOptions());
        defaultCFHandle = columnFamilyHandles.remove(0);
        SimpleMetricRegistry registry = new SimpleMetricRegistry();
        SimpleMetricGroup group = new SimpleMetricGroup(registry);

        long slidingWindowSize = Time.days(1).toMilliseconds();
        RocksDBAccessMetric.Builder builder =
                new RocksDBAccessMetric.Builder()
                        .setEnabled(true)
                        .setSampleInterval(sampleInterval)
                        .setHistogramSlidingWindow(slidingWindowSize)
                        .setMetricGroup(group);
        stateBackend =
                RocksDBTestUtils.builderForTestDB(
                                TEMP_FOLDER.newFolder(),
                                IntSerializer.INSTANCE,
                                db,
                                defaultCFHandle,
                                optionsContainer.getColumnOptions(),
                                group)
                        .setAccessMetricBuilder(builder)
                        .build();
        RocksDBAccessMetric accessMetric = stateBackend.db.getAccessMetric();
        Preconditions.checkNotNull(accessMetric);
        Map<Integer, Map<String, Histogram>> histogramMetrics = accessMetric.getHistogramMetrics();

        ValueStateDescriptor<String> valueStateDescriptor =
                new ValueStateDescriptor<>("value-state", StringSerializer.INSTANCE);
        valueState =
                stateBackend.createInternalState(
                        VoidNamespaceSerializer.INSTANCE, valueStateDescriptor);
        Map.Entry<Integer, Map<String, Histogram>> next =
                histogramMetrics.entrySet().iterator().next();
        int valueStateId = next.getKey();
        valueStateHistogram = next.getValue();

        ListStateDescriptor<String> listStateDescriptor =
                new ListStateDescriptor<>("list-state", StringSerializer.INSTANCE);
        listState =
                stateBackend.createInternalState(
                        VoidNamespaceSerializer.INSTANCE, listStateDescriptor);
        int listStateId = 0;
        for (Map.Entry<Integer, Map<String, Histogram>> entry : histogramMetrics.entrySet()) {
            if (entry.getKey() != valueStateId) {
                listStateId = entry.getKey();
                listStateHistogram = entry.getValue();
            }
        }

        MapStateDescriptor<Integer, String> mapStateDescriptor =
                new MapStateDescriptor<>(
                        "map-state", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        mapState =
                stateBackend.createInternalState(
                        VoidNamespaceSerializer.INSTANCE, mapStateDescriptor);
        for (Map.Entry<Integer, Map<String, Histogram>> entry : histogramMetrics.entrySet()) {
            if (entry.getKey() != valueStateId && entry.getKey() != listStateId) {
                mapStateHistogram = entry.getValue();
            }
        }
    }

    @After
    public void cleanupRocksDB() {
        IOUtils.closeQuietly(stateBackend);
        if (stateBackend != null) {
            stateBackend.dispose();
        }
        IOUtils.closeQuietly(defaultCFHandle);
        IOUtils.closeQuietly(db);
        IOUtils.closeQuietly(optionsContainer);
    }

    @Test
    public void testTrackPutLatency() throws Exception {
        stateBackend.setCurrentKey(-1);
        String value = StringUtils.getRandomString(ThreadLocalRandom.current(), 1, 10);
        for (int i = 0; i < sampleInterval; i++) {
            valueState.update(value);
            Assert.assertEquals(1, valueStateHistogram.get(PUT_LATENCY).getCount());
        }
        valueState.update(value);
        Assert.assertEquals(2, valueStateHistogram.get(PUT_LATENCY).getCount());

        for (int i = 0; i < sampleInterval; i++) {
            mapState.put(ThreadLocalRandom.current().nextInt(), value);
            Assert.assertEquals(1, mapStateHistogram.get(PUT_LATENCY).getCount());
        }
        mapState.put(ThreadLocalRandom.current().nextInt(), value);
        Assert.assertEquals(2, mapStateHistogram.get(PUT_LATENCY).getCount());
    }

    @Test
    public void testTrackGetLatency() throws Exception {
        stateBackend.setCurrentKey(-1);
        String value = StringUtils.getRandomString(ThreadLocalRandom.current(), 1, 10);
        valueState.update(value);
        for (int i = 0; i < sampleInterval; i++) {
            valueState.value();
            Assert.assertEquals(1, valueStateHistogram.get(GET_LATENCY).getCount());
        }
        valueState.value();
        Assert.assertEquals(2, valueStateHistogram.get(GET_LATENCY).getCount());

        listState.add(value);
        for (int i = 0; i < sampleInterval; i++) {
            listState.get();
            Assert.assertEquals(1, listStateHistogram.get(GET_LATENCY).getCount());
        }
        listState.get();
        Assert.assertEquals(2, listStateHistogram.get(GET_LATENCY).getCount());

        mapState.put(ThreadLocalRandom.current().nextInt(), value);
        for (int i = 0; i < sampleInterval; i++) {
            mapState.get(ThreadLocalRandom.current().nextInt());
            Assert.assertEquals(1, mapStateHistogram.get(GET_LATENCY).getCount());
        }
        mapState.get(ThreadLocalRandom.current().nextInt());
        Assert.assertEquals(2, mapStateHistogram.get(GET_LATENCY).getCount());
    }

    @Test
    public void testTrackWriteBatchLatency() throws Exception {
        stateBackend.setCurrentKey(-1);
        String value = StringUtils.getRandomString(ThreadLocalRandom.current(), 1, 10);

        for (int i = 0; i < sampleInterval; i++) {
            mapState.putAll(Collections.singletonMap(ThreadLocalRandom.current().nextInt(), value));
            Assert.assertEquals(1, mapStateHistogram.get(WRITE_BATCH_LATENCY).getCount());
        }
        mapState.putAll(Collections.singletonMap(ThreadLocalRandom.current().nextInt(), value));
        Assert.assertEquals(2, mapStateHistogram.get(WRITE_BATCH_LATENCY).getCount());
    }

    @Test
    public void testTrackMergeLatency() throws Exception {
        stateBackend.setCurrentKey(-1);
        String value = StringUtils.getRandomString(ThreadLocalRandom.current(), 1, 10);

        for (int i = 0; i < sampleInterval / 2; i++) {
            listState.add(value);
            listState.addAll(Collections.singletonList(value));
            Assert.assertEquals(1, listStateHistogram.get(MERGE_LATENCY).getCount());
        }
        listState.add(value);
        listState.addAll(Collections.singletonList(value));
        Assert.assertEquals(2, listStateHistogram.get(MERGE_LATENCY).getCount());
    }

    @Test
    public void testTrackDeleteLatency() throws Exception {
        stateBackend.setCurrentKey(-1);
        String value = StringUtils.getRandomString(ThreadLocalRandom.current(), 1, 10);

        valueState.update(value);
        for (int i = 0; i < sampleInterval; i++) {
            valueState.clear();
            Assert.assertEquals(1, valueStateHistogram.get(DELETE_LATENCY).getCount());
        }
        valueState.clear();
        Assert.assertEquals(2, valueStateHistogram.get(DELETE_LATENCY).getCount());

        listState.add(value);
        for (int i = 0; i < sampleInterval; i++) {
            listState.clear();
            Assert.assertEquals(1, listStateHistogram.get(DELETE_LATENCY).getCount());
        }
        listState.clear();
        Assert.assertEquals(2, listStateHistogram.get(DELETE_LATENCY).getCount());

        mapState.put(ThreadLocalRandom.current().nextInt(), value);
        for (int i = 0; i < sampleInterval; i++) {
            mapState.remove(ThreadLocalRandom.current().nextInt());
            Assert.assertEquals(1, mapStateHistogram.get(DELETE_LATENCY).getCount());
        }
        mapState.remove(ThreadLocalRandom.current().nextInt());
        Assert.assertEquals(2, mapStateHistogram.get(DELETE_LATENCY).getCount());
    }

    @Test
    public void testTrackSeekAndNextLatency() throws Exception {
        stateBackend.setCurrentKey(-1);
        String value = StringUtils.getRandomString(ThreadLocalRandom.current(), 1, 10);

        mapState.put(ThreadLocalRandom.current().nextInt(), value);
        for (int i = 0; i < sampleInterval / 4; i++) {
            mapState.iterator().hasNext();
            mapState.entries().iterator().hasNext();
            mapState.values().iterator().hasNext();
            mapState.keys().iterator().hasNext();
            Assert.assertEquals(1, mapStateHistogram.get(SEEK_LATENCY).getCount());
            Assert.assertEquals(1, mapStateHistogram.get(NEXT_LATENCY).getCount());
        }
        mapState.iterator().hasNext();
        mapState.entries().iterator().hasNext();
        mapState.values().iterator().hasNext();
        mapState.keys().iterator().hasNext();
        Assert.assertEquals(2, mapStateHistogram.get(SEEK_LATENCY).getCount());
        Assert.assertEquals(2, mapStateHistogram.get(NEXT_LATENCY).getCount());
    }

    private static class SimpleMetricRegistry implements MetricRegistry {

        @Override
        public char getDelimiter() {
            return 0;
        }

        @Override
        public int getNumberReporters() {
            return 0;
        }

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {}

        @Override
        public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {}

        @Override
        public ScopeFormats getScopeFormats() {
            Configuration config = new Configuration();

            return ScopeFormats.fromConfig(config);
        }
    }

    private static class SimpleMetricGroup extends AbstractMetricGroup {

        public SimpleMetricGroup(MetricRegistry registry) {
            super(registry, new String[0], null);
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return "test";
        }

        @Override
        protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
            return null;
        }
    }
}
