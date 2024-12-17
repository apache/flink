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

package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.runtime.arrow.ByteArrayUtils;
import org.apache.flink.table.types.DataType;

/** A {@link ScanTableSource} for serialized arrow record batch data. */
@Internal
public class ArrowTableSource implements ScanTableSource {

    private final DataType dataType;

    private final byte[][] arrowData;

    public ArrowTableSource(DataType dataType, String data) {
        this.dataType = dataType;
        try {
            this.arrowData = ByteArrayUtils.stringToTwoDimByteArray(data);
        } catch (Throwable e) {
            throw new TableException(
                    "Failed to convert the data from String to byte[][].\nThe data is: " + data, e);
        }
    }

    private ArrowTableSource(DataType dataType, byte[][] arrowData) {
        this.dataType = dataType;
        this.arrowData = arrowData;
    }

    @Override
    public DynamicTableSource copy() {
        return new ArrowTableSource(dataType, arrowData);
    }

    @Override
    public String asSummaryString() {
        return "ArrowTableSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceFunctionProvider.of(new ArrowSourceFunction(dataType, arrowData), true);
    }
}
