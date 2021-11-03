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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.DecimalColumnVector;
import org.apache.flink.table.data.columnar.vector.IntColumnVector;
import org.apache.flink.table.data.columnar.vector.LongColumnVector;

/**
 * Parquet write decimal as int32 and int64 and binary, this class wrap the real vector to provide
 * {@link DecimalColumnVector} interface.
 */
public class ParquetDecimalVector implements DecimalColumnVector {

    public final ColumnVector vector;

    public ParquetDecimalVector(ColumnVector vector) {
        this.vector = vector;
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        if (ParquetSchemaConverter.is32BitDecimal(precision) && vector instanceof IntColumnVector) {
            return DecimalData.fromUnscaledLong(
                    ((IntColumnVector) vector).getInt(i), precision, scale);
        } else if (ParquetSchemaConverter.is64BitDecimal(precision)
                && vector instanceof LongColumnVector) {
            return DecimalData.fromUnscaledLong(
                    ((LongColumnVector) vector).getLong(i), precision, scale);
        } else {
            return DecimalData.fromUnscaledBytes(
                    ((BytesColumnVector) vector).getBytes(i).getBytes(), precision, scale);
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNullAt(i);
    }
}
