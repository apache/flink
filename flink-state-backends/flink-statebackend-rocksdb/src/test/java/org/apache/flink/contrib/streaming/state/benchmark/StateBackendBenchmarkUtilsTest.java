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

package org.apache.flink.contrib.streaming.state.benchmark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.StateBackendType;
import org.apache.flink.contrib.streaming.state.writer.WriteBatchMechanism;
import org.apache.flink.runtime.state.KeyedStateBackend;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.applyToAllKeys;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.cleanUp;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.compactState;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.createKeyedStateBackend;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.getListState;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.getMapState;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.getValueState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

/** Test for {@link StateBackendBenchmarkUtils}. */
@RunWith(Parameterized.class)
public class StateBackendBenchmarkUtilsTest {
    @Rule public TemporaryFolder temp = new TemporaryFolder();

    private final ValueStateDescriptor<Long> valueStateDescriptor =
            new ValueStateDescriptor<>("valueState", Long.class);
    private final ListStateDescriptor<Long> listStateDescriptor =
            new ListStateDescriptor<>("listState", Long.class);
    private final MapStateDescriptor<Long, Double> mapStateDescriptor =
            new MapStateDescriptor<>("mapState", Long.class, Double.class);

    @Parameters(name = "StateBackendType={0} WriteBatchMechanism={1}")
    public static Collection<Object[]> data() {
        // Create test parameters for each combination of StateBackendType and
        // for each WriteBatchMechanism for the RocksDB StateBackendType.
        List<Object[]> parameters = new ArrayList<>();
        for (StateBackendType value : StateBackendBenchmarkUtils.StateBackendType.values()) {
            if (value == StateBackendType.ROCKSDB) {
                for (WriteBatchMechanism writeBatchMechanism : WriteBatchMechanism.values()) {
                    parameters.add(
                            new Object[] {
                                value, writeBatchMechanism,
                            });
                }
            } else {
                parameters.add(new Object[] {value, null});
            }
        }
        return parameters;
    }

    @Parameter() public StateBackendBenchmarkUtils.StateBackendType backendType;

    @Parameter(1)
    public WriteBatchMechanism writeBatchMechanism;

    @Test
    public void testCreateKeyedStateBackend() throws IOException {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType, writeBatchMechanism);
        cleanUp(backend);
    }

    @Test
    public void testGetValueState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType, writeBatchMechanism);
        getValueState(backend, valueStateDescriptor);
        cleanUp(backend);
    }

    @Test
    public void testGetListState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType, writeBatchMechanism);
        getListState(backend, listStateDescriptor);
        cleanUp(backend);
    }

    @Test
    public void testGetMapState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType, writeBatchMechanism);
        getMapState(backend, mapStateDescriptor);
        cleanUp(backend);
    }

    @Test
    public void testApplyToAllKeys() throws Exception {
        Assume.assumeThat(
                backendType,
                not(equalTo(StateBackendBenchmarkUtils.StateBackendType.BATCH_EXECUTION)));
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType, writeBatchMechanism);
        ListState<Long> listState = getListState(backend, listStateDescriptor);
        for (long i = 0; i < 10; i++) {
            backend.setCurrentKey(i);
            listState.add(i);
        }
        applyToAllKeys(
                backend,
                listStateDescriptor,
                (k, state) -> {
                    backend.setCurrentKey(k);
                    state.clear();
                });
        for (long i = 0; i < 10; i++) {
            backend.setCurrentKey(i);
            Assert.assertNull(listState.get());
        }
        cleanUp(backend);
    }

    @Test
    public void testCompactState() throws Exception {
        KeyedStateBackend<Long> backend = createKeyedStateBackend(backendType, writeBatchMechanism);
        ListState<Long> listState = getListState(backend, listStateDescriptor);
        for (long i = 0; i < 10; i++) {
            backend.setCurrentKey(i);
            listState.add(i);
        }
        if (backend instanceof RocksDBKeyedStateBackend) {
            RocksDBKeyedStateBackend<Long> rocksDBKeyedStateBackend =
                    (RocksDBKeyedStateBackend<Long>) backend;
            compactState(rocksDBKeyedStateBackend, listStateDescriptor);
        }
        cleanUp(backend);
    }
}
