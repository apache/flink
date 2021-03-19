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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Random;

/** Generate random keys. */
class RandomKeyGeneratorFactory {

    private static final Random rnd = new Random();

    private static final double NULL_PROBABILITY = 0.1;

    static RandomKeyGenerator<BinaryRowData> createBinaryRowDataKeyGenerator() {
        return (fieldTypes, num) -> {
            BinaryRowData[] rows = new BinaryRowData[num];
            for (int i = 0; i < num; i++) {
                rows[i] = createRow(fieldTypes);
            }
            return rows;
        };
    }

    static RandomKeyGenerator<WindowKey> createWindowKeyGenerator() {
        return (fieldTypes, num) -> {
            WindowKey[] windowKeys = new WindowKey[num];
            for (int i = 0; i < num; i++) {
                windowKeys[i] = new WindowKey(rnd.nextLong(), createRow(fieldTypes));
            }
            return windowKeys;
        };
    }

    private static BinaryRowData createRow(LogicalType[] types) {

        BinaryRowData row = new BinaryRowData(types.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        int intVal = rnd.nextInt(Integer.MAX_VALUE);

        for (int i = 0; i < types.length; i++) {
            if (rnd.nextDouble() < NULL_PROBABILITY) {
                writer.setNullAt(i);
                continue;
            }

            if (types[i] instanceof IntType) {
                writer.writeInt(i, rnd.nextInt());
            } else if (types[i] instanceof VarCharType) {
                writer.writeString(i, StringData.fromString(getString(intVal, intVal % 1024)));
            } else if (types[i] instanceof DoubleType) {
                writer.writeDouble(i, rnd.nextDouble());
            } else if (types[i] instanceof BigIntType) {
                writer.writeLong(i, rnd.nextLong());
            } else if (types[i] instanceof BooleanType) {
                writer.writeBoolean(i, rnd.nextBoolean());
            } else if (types[i] instanceof FloatType) {
                writer.writeFloat(i, rnd.nextFloat());
            } else if (types[i] instanceof SmallIntType) {
                writer.writeShort(i, (short) rnd.nextInt());
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + types[i]);
            }
        }
        return row;
    }

    private static String getString(int count, int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append(count);
        }
        return builder.toString();
    }
}
