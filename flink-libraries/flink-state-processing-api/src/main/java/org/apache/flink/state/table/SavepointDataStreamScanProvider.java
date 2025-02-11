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

package org.apache.flink.state.table;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.naming.ConfigurationException;

import java.util.List;

/** State data stream scan provider. */
public class SavepointDataStreamScanProvider implements DataStreamScanProvider {
    private final String stateBackendType;
    private final String statePath;
    private final OperatorIdentifier operatorIdentifier;
    private final String keyFormat;
    private final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueProjections;
    private final RowType rowType;

    public SavepointDataStreamScanProvider(
            final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final String keyFormat,
            final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueProjections,
            RowType rowType) {
        this.stateBackendType = stateBackendType;
        this.statePath = statePath;
        this.operatorIdentifier = operatorIdentifier;
        this.keyFormat = keyFormat;
        this.keyValueProjections = keyValueProjections;
        this.rowType = rowType;
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        try {
            Configuration configuration = new Configuration();
            configuration.set(StateBackendOptions.STATE_BACKEND, stateBackendType);
            StateBackend stateBackend =
                    StateBackendLoader.loadStateBackendFromConfig(
                            configuration, getClass().getClassLoader(), null);

            SavepointReader savepointReader =
                    SavepointReader.read(execEnv, statePath, stateBackend);

            // Get key type information
            TypeInformation keyTypeInfo = TypeInformation.of(Class.forName(keyFormat));

            // Get value state descriptors
            for (StateValueColumnConfiguration columnConfig : keyValueProjections.f1) {
                TypeInformation valueTypeInfo =
                        TypeInformation.of(Class.forName(columnConfig.getValueFormat()));

                switch (columnConfig.getStateType()) {
                    case VALUE:
                        columnConfig.setStateDescriptor(
                                new ValueStateDescriptor<>(
                                        columnConfig.getStateName(), valueTypeInfo));
                        break;

                    case LIST:
                        columnConfig.setStateDescriptor(
                                new ListStateDescriptor<>(
                                        columnConfig.getStateName(), valueTypeInfo));
                        break;

                    case MAP:
                        if (columnConfig.getMapKeyFormat() == null) {
                            throw new ConfigurationException(
                                    "Map key format is required for map state");
                        }
                        TypeInformation<?> mapKeyTypeInfo =
                                TypeInformation.of(Class.forName(columnConfig.getMapKeyFormat()));
                        columnConfig.setStateDescriptor(
                                new MapStateDescriptor<>(
                                        columnConfig.getStateName(),
                                        mapKeyTypeInfo,
                                        valueTypeInfo));
                        break;

                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported state type: " + columnConfig.getStateType());
                }
            }

            TypeInformation outTypeInfo = InternalTypeInfo.of(rowType);

            return savepointReader.readKeyedState(
                    operatorIdentifier,
                    new KeyedStateReader(rowType, keyValueProjections),
                    keyTypeInfo,
                    outTypeInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
