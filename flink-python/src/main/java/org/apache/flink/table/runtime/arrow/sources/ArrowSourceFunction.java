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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * An Arrow {@link SourceFunction} which takes {@link RowData} as the type of the produced records.
 */
@Internal
public class ArrowSourceFunction extends AbstractArrowSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;

    ArrowSourceFunction(DataType dataType, byte[][] arrowData) {
        super(dataType, arrowData);
    }

    @Override
    ArrowReader<RowData> createArrowReader(VectorSchemaRoot root) {
        return ArrowUtils.createRowDataArrowReader(root, (RowType) dataType.getLogicalType());
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return (TypeInformation<RowData>)
                TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(dataType);
    }
}
