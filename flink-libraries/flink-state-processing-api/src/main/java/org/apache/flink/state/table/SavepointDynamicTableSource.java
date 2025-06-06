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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** Savepoint dynamic source. */
@SuppressWarnings("rawtypes")
public class SavepointDynamicTableSource implements ScanTableSource {
    @Nullable private final String stateBackendType;
    private final String statePath;
    private final OperatorIdentifier operatorIdentifier;
    private final TypeInformation keyTypeInfo;
    private final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueProjections;
    private final RowType rowType;

    public SavepointDynamicTableSource(
            @Nullable final String stateBackendType,
            final String statePath,
            final OperatorIdentifier operatorIdentifier,
            final TypeInformation keyTypeInfo,
            final Tuple2<Integer, List<StateValueColumnConfiguration>> keyValueProjections,
            RowType rowType) {
        this.stateBackendType = stateBackendType;
        this.statePath = statePath;
        this.operatorIdentifier = operatorIdentifier;
        this.keyValueProjections = keyValueProjections;
        this.keyTypeInfo = keyTypeInfo;
        this.rowType = rowType;
    }

    @Override
    public DynamicTableSource copy() {
        return new SavepointDynamicTableSource(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyTypeInfo,
                keyValueProjections,
                rowType);
    }

    @Override
    public String asSummaryString() {
        return "Savepoint Table Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return new SavepointDataStreamScanProvider(
                stateBackendType,
                statePath,
                operatorIdentifier,
                keyTypeInfo,
                keyValueProjections,
                rowType);
    }
}
