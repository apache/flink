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

package org.apache.flink.table.utils.python;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.python.PythonBridgeUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;

/** Implementation of {@link ScanTableSource} for python elements table. */
public class PythonDynamicTableSource implements ScanTableSource {
    private final String filePath;
    private final boolean batched;
    private final DataType producedDataType;

    public PythonDynamicTableSource(String filePath, boolean batched, DataType producedDataType) {
        this.filePath = filePath;
        this.batched = batched;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        return new PythonDynamicTableSource(filePath, batched, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Python Table Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        try {
            InputFormat<RowData, ?> inputFormat =
                    PythonTableUtils.getInputFormat(
                            PythonBridgeUtils.readPythonObjects(filePath, batched),
                            producedDataType);
            return InputFormatProvider.of(inputFormat);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to read input data from %s.", filePath), e);
        }
    }
}
