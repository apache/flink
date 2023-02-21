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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

/** A {@link StreamTableSource} for serialized arrow record batch data. */
@Internal
public class ArrowTableSource implements StreamTableSource<RowData> {

    final DataType dataType;
    final byte[][] arrowData;

    public ArrowTableSource(DataType dataType, byte[][] arrowData) {
        this.dataType = dataType;
        this.arrowData = arrowData;
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(new ArrowSourceFunction(dataType, arrowData));
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromResolvedSchema(DataTypeUtils.expandCompositeTypeToSchema(dataType));
    }

    @Override
    public DataType getProducedDataType() {
        return dataType;
    }
}
