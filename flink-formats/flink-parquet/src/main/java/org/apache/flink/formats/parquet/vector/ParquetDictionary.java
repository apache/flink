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

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.Dictionary;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.decodeInt64ToTimestamp;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.decodeInt96ToTimestamp;

/** Parquet dictionary. */
public final class ParquetDictionary implements Dictionary {

    private org.apache.parquet.column.Dictionary dictionary;
    private final ColumnDescriptor descriptor;

    public ParquetDictionary(
            org.apache.parquet.column.Dictionary dictionary, ColumnDescriptor descriptor) {
        this.dictionary = dictionary;
        this.descriptor = descriptor;
    }

    @Override
    public int decodeToInt(int id) {
        return dictionary.decodeToInt(id);
    }

    @Override
    public long decodeToLong(int id) {
        return dictionary.decodeToLong(id);
    }

    @Override
    public float decodeToFloat(int id) {
        return dictionary.decodeToFloat(id);
    }

    @Override
    public double decodeToDouble(int id) {
        return dictionary.decodeToDouble(id);
    }

    @Override
    public byte[] decodeToBinary(int id) {
        return dictionary.decodeToBinary(id).getBytes();
    }

    @Override
    public TimestampData decodeToTimestamp(int id) {
        if (descriptor.getPrimitiveType().getPrimitiveTypeName()
                == PrimitiveType.PrimitiveTypeName.INT64) {
            return decodeInt64ToTimestamp(
                    true,
                    dictionary,
                    id,
                    ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                                    descriptor.getPrimitiveType().getLogicalTypeAnnotation())
                            .getUnit());
        }
        return decodeInt96ToTimestamp(true, dictionary, id);
    }
}
