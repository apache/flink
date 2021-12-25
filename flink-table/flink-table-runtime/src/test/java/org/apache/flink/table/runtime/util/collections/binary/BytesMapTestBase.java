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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Random;

/** Test case for {@link BytesMap}. */
public class BytesMapTestBase {
    protected static final long RANDOM_SEED = 76518743207143L;
    protected static final int PAGE_SIZE = 32 * 1024;
    protected static final int NUM_ENTRIES = 10000;

    protected BinaryRowData[] getRandomizedInputs(int num) {
        final Random rnd = new Random(RANDOM_SEED);
        return getRandomizedInputs(num, rnd, true);
    }

    protected BinaryRowData[] getRandomizedInputs(int num, Random rnd, boolean nullable) {
        BinaryRowData[] lists = new BinaryRowData[num];
        for (int i = 0; i < num; i++) {
            int intVal = rnd.nextInt(Integer.MAX_VALUE);
            long longVal = -rnd.nextLong();
            boolean boolVal = longVal % 2 == 0;
            String strVal = nullable && boolVal ? null : getString(intVal, intVal % 1024) + i;
            Double doubleVal = rnd.nextDouble();
            Short shotVal = (short) intVal;
            Float floatVal = nullable && boolVal ? null : rnd.nextFloat();
            lists[i] = createRow(intVal, strVal, doubleVal, longVal, boolVal, floatVal, shotVal);
        }
        return lists;
    }

    protected BinaryRowData createRow(
            Integer f0, String f1, Double f2, Long f3, Boolean f4, Float f5, Short f6) {

        BinaryRowData row = new BinaryRowData(7);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        // int, string, double, long, boolean
        if (f0 == null) {
            writer.setNullAt(0);
        } else {
            writer.writeInt(0, f0);
        }
        if (f1 == null) {
            writer.setNullAt(1);
        } else {
            writer.writeString(1, StringData.fromString(f1));
        }
        if (f2 == null) {
            writer.setNullAt(2);
        } else {
            writer.writeDouble(2, f2);
        }
        if (f3 == null) {
            writer.setNullAt(3);
        } else {
            writer.writeLong(3, f3);
        }
        if (f4 == null) {
            writer.setNullAt(4);
        } else {
            writer.writeBoolean(4, f4);
        }
        if (f5 == null) {
            writer.setNullAt(5);
        } else {
            writer.writeFloat(5, f5);
        }
        if (f6 == null) {
            writer.setNullAt(6);
        } else {
            writer.writeShort(6, f6);
        }
        writer.complete();
        return row;
    }

    protected int needNumMemSegments(int numEntries, int valLen, int keyLen, int pageSize) {
        return 2 * (valLen + keyLen + 1024 * 3 + 4 + 8 + 8) * numEntries / pageSize;
    }

    protected int rowLength(RowType tpe) {
        return BinaryRowData.calculateFixPartSizeInBytes(tpe.getFieldCount())
                + BytesHashMap.getVariableLength(tpe.getChildren().toArray(new LogicalType[0]));
    }

    private String getString(int count, int length) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append(count);
        }
        return builder.toString();
    }
}
