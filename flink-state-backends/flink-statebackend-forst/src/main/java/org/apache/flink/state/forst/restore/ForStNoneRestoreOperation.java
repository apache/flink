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

package org.apache.flink.state.forst.restore;

import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.forst.ForStDBTtlCompactFiltersManager;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;

import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.Map;
import java.util.function.Function;

/** Encapsulates the process of initiating a ForSt instance without restore. */
public class ForStNoneRestoreOperation implements ForStRestoreOperation {
    private final ForStHandle rocksHandle;

    public ForStNoneRestoreOperation(
            Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            Path instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ForStNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull ForStDBTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity) {
        this.rocksHandle =
                new ForStHandle(
                        kvStateInformation,
                        instanceRocksDBPath,
                        dbOptions,
                        columnFamilyOptionsFactory,
                        nativeMetricOptions,
                        metricGroup,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);
    }

    @Override
    public ForStRestoreResult restore() throws Exception {
        this.rocksHandle.openDB();
        return new ForStRestoreResult(
                this.rocksHandle.getDb(),
                this.rocksHandle.getDefaultColumnFamilyHandle(),
                this.rocksHandle.getNativeMetricMonitor(),
                -1,
                null,
                null);
    }

    @Override
    public void close() {
        this.rocksHandle.close();
    }
}
