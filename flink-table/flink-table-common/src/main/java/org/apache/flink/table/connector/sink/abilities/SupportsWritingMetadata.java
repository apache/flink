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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for {@link DynamicTableSink}s that support writing metadata columns.
 *
 * <p>Metadata columns add additional columns to the table's schema. A table sink is responsible for
 * accepting requested metadata columns at the end of consumed rows and persist them. This includes
 * potentially forwarding metadata columns to contained formats.
 *
 * <p>Examples in SQL look like:
 *
 * <pre>{@code
 * // writes data to the corresponding metadata key `timestamp`
 * CREATE TABLE t1 (i INT, s STRING, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA, d DOUBLE)
 *
 * // casts data from INT and writes to metadata key `timestamp`
 * CREATE TABLE t2 (i INT, s STRING, myTimestamp INT METADATA FROM 'timestamp', d DOUBLE)
 *
 * // metadata is not persisted because metadata column is virtual
 * CREATE TABLE t3 (i INT, s STRING, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA VIRTUAL, d DOUBLE)
 * }</pre>
 *
 * <p>By default, if this interface is not implemented, the statements above would fail because the
 * table sink does not provide a metadata key called `timestamp`.
 *
 * <p>If this interface is implemented, {@link #listWritableMetadata()} lists all metadata keys and
 * their corresponding data types that the sink exposes to the planner. The planner will use this
 * information for validation and insertion of explicit casts if necessary.
 *
 * <p>The planner will select required metadata columns and will call {@link
 * #applyWritableMetadata(List, DataType)} with a list of metadata keys. An implementation must
 * ensure that metadata columns are accepted at the end of the physical row in the order of the
 * provided list after the apply method has been called.
 *
 * <p>The metadata column's data type must match with {@link #listWritableMetadata()}. For the
 * examples above, this means that a table sink for `t2` accepts a TIMESTAMP and not INT. The
 * casting from INT will be performed by the planner in a preceding operation:
 *
 * <pre>{@code
 * // for t1 and t2
 * ROW < i INT, s STRING, d DOUBLE >                                              // physical input
 * ROW < i INT, s STRING, d DOUBLE, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE > // final input
 *
 * // for t3
 * ROW < i INT, s STRING, d DOUBLE >                                              // physical input
 * ROW < i INT, s STRING, d DOUBLE >                                              // final input
 * }</pre>
 */
@PublicEvolving
public interface SupportsWritingMetadata {

    /**
     * Returns the map of metadata keys and their corresponding data types that can be consumed by
     * this table sink for writing.
     *
     * <p>The returned map will be used by the planner for validation and insertion of explicit
     * casts (see {@link LogicalTypeCasts#supportsExplicitCast(LogicalType, LogicalType)}) if
     * necessary.
     *
     * <p>The iteration order of the returned map determines the order of metadata keys in the list
     * passed in {@link #applyWritableMetadata(List, DataType)}. Therefore, it might be beneficial
     * to return a {@link LinkedHashMap} if a strict metadata column order is required.
     *
     * <p>If a sink forwards metadata to one or more formats, we recommend the following column
     * order for consistency:
     *
     * <pre>{@code
     * KEY FORMAT METADATA COLUMNS + VALUE FORMAT METADATA COLUMNS + SINK METADATA COLUMNS
     * }</pre>
     *
     * <p>Metadata key names follow the same pattern as mentioned in {@link Factory}. In case of
     * duplicate names in format and sink keys, format keys shall have higher precedence.
     *
     * <p>Regardless of the returned {@link DataType}s, a metadata column is always represented
     * using internal data structures (see {@link RowData}).
     *
     * @see EncodingFormat#listWritableMetadata()
     */
    Map<String, DataType> listWritableMetadata();

    /**
     * Provides a list of metadata keys that the consumed {@link RowData} will contain as appended
     * metadata columns which must be persisted.
     *
     * @param metadataKeys a subset of the keys returned by {@link #listWritableMetadata()}, ordered
     *     by the iteration order of returned map
     * @param consumedDataType the final input type of the sink
     * @see EncodingFormat#applyWritableMetadata(List)
     */
    void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType);
}
