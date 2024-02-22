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

package org.apache.flink.table.runtime.operators.window.tvf.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.ZoneId;
import java.util.Arrays;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;

/** A test base to test window table function operator . */
public abstract class WindowTableFunctionOperatorTestBase {

    protected static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    protected static final ZoneId SHANGHAI_ZONE_ID = ZoneId.of("Asia/Shanghai");
    protected final ZoneId shiftTimeZone;

    public WindowTableFunctionOperatorTestBase(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
    }

    /** Get the timestamp in mills by given epoch mills and timezone. */
    protected long localMills(long epochMills) {
        return toUtcTimestampMills(epochMills, shiftTimeZone);
    }

    // ============================== Utils ==============================

    // ============================== Util Fields ==============================

    protected static final RowType INPUT_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("f0", new VarCharType(Integer.MAX_VALUE)),
                            new RowType.RowField("f1", new IntType()),
                            new RowType.RowField("f2", new TimestampType(3))));

    protected static final RowDataSerializer INPUT_ROW_SER = new RowDataSerializer(INPUT_ROW_TYPE);
    protected static final int ROW_TIME_INDEX = 2;

    protected static final LogicalType[] OUTPUT_TYPES =
            new LogicalType[] {
                new VarCharType(Integer.MAX_VALUE),
                new IntType(),
                new TimestampType(3),
                new TimestampType(3),
                new TimestampType(3),
                new TimestampType(3)
            };

    protected static final TypeSerializer<RowData> OUT_SERIALIZER =
            new RowDataSerializer(OUTPUT_TYPES);

    protected static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES, new GenericRowRecordSortComparator(4, new TimestampType()));

    // ============================== Util Functions ==============================

    protected static StreamRecord<RowData> insertRecord(String f0, int f1, Long... f2) {
        Object[] fields = new Object[2 + f2.length];
        fields[0] = f0;
        fields[1] = f1;
        for (int idx = 0; idx < f2.length; idx++) {
            if (f2[idx] == null) {
                fields[2 + idx] = null;
            } else {
                fields[2 + idx] = TimestampData.fromEpochMillis(f2[idx]);
            }
        }
        return new StreamRecord<>(row(fields));
    }
}
