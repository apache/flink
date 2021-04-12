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

package org.apache.flink.orc.vector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.sql.Timestamp;

/** A {@link Vectorizer} of {@link RowData} type element. */
public class RowDataVectorizer extends Vectorizer<RowData> {

    private final LogicalType[] fieldTypes;

    public RowDataVectorizer(String schema, LogicalType[] fieldTypes) {
        super(schema);
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void vectorize(RowData row, VectorizedRowBatch batch) {
        int rowId = batch.size++;
        for (int i = 0; i < row.getArity(); ++i) {
            setColumn(rowId, batch.cols[i], fieldTypes[i], row, i);
        }
    }

    private static void setColumn(
            int rowId, ColumnVector column, LogicalType type, RowData row, int columnId) {
        if (row.isNullAt(columnId)) {
            column.noNulls = false;
            column.isNull[rowId] = true;
            return;
        }

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                {
                    BytesColumnVector vector = (BytesColumnVector) column;
                    byte[] bytes = row.getString(columnId).toBytes();
                    vector.setVal(rowId, bytes, 0, bytes.length);
                    break;
                }
            case BOOLEAN:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getBoolean(columnId) ? 1 : 0;
                    break;
                }
            case BINARY:
            case VARBINARY:
                {
                    BytesColumnVector vector = (BytesColumnVector) column;
                    byte[] bytes = row.getBinary(columnId);
                    vector.setVal(rowId, bytes, 0, bytes.length);
                    break;
                }
            case DECIMAL:
                {
                    DecimalType dt = (DecimalType) type;
                    DecimalColumnVector vector = (DecimalColumnVector) column;
                    vector.set(
                            rowId,
                            HiveDecimal.create(
                                    row.getDecimal(columnId, dt.getPrecision(), dt.getScale())
                                            .toBigDecimal()));
                    break;
                }
            case TINYINT:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getByte(columnId);
                    break;
                }
            case SMALLINT:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getShort(columnId);
                    break;
                }
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getInt(columnId);
                    break;
                }
            case BIGINT:
                {
                    LongColumnVector vector = (LongColumnVector) column;
                    vector.vector[rowId] = row.getLong(columnId);
                    break;
                }
            case FLOAT:
                {
                    DoubleColumnVector vector = (DoubleColumnVector) column;
                    vector.vector[rowId] = row.getFloat(columnId);
                    break;
                }
            case DOUBLE:
                {
                    DoubleColumnVector vector = (DoubleColumnVector) column;
                    vector.vector[rowId] = row.getDouble(columnId);
                    break;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    TimestampType tt = (TimestampType) type;
                    Timestamp timestamp =
                            row.getTimestamp(columnId, tt.getPrecision()).toTimestamp();
                    TimestampColumnVector vector = (TimestampColumnVector) column;
                    vector.set(rowId, timestamp);
                    break;
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    LocalZonedTimestampType lt = (LocalZonedTimestampType) type;
                    Timestamp timestamp =
                            row.getTimestamp(columnId, lt.getPrecision()).toTimestamp();
                    TimestampColumnVector vector = (TimestampColumnVector) column;
                    vector.set(rowId, timestamp);
                    break;
                }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
