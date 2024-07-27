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

package org.apache.flink.table.runtime.operators.aggregate.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.assertj.core.api.Assertions.fail;

/** A test base for window aggregate operator. */
public abstract class WindowAggOperatorTestBase {

    protected static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    protected static final ZoneId SHANGHAI_ZONE_ID = ZoneId.of("Asia/Shanghai");

    protected final ZoneId shiftTimeZone;

    public WindowAggOperatorTestBase(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
    }

    /** Get the timestamp in mills by given epoch mills and timezone. */
    protected long localMills(long epochMills) {
        return toUtcTimestampMills(epochMills, shiftTimeZone);
    }

    // ============================== Utils ==============================

    // ============================== Util Fields ==============================

    private static final RowType INPUT_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("f0", new VarCharType(Integer.MAX_VALUE)),
                            new RowType.RowField("f1", new IntType()),
                            new RowType.RowField("f2", new TimestampType())));

    protected static final RowDataSerializer INPUT_ROW_SER = new RowDataSerializer(INPUT_ROW_TYPE);

    protected static final RowDataSerializer ACC_SER =
            new RowDataSerializer(new BigIntType(), new BigIntType());

    protected static final LogicalType[] OUTPUT_TYPES =
            new LogicalType[] {
                new VarCharType(Integer.MAX_VALUE),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType()
            };

    protected static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, INPUT_ROW_TYPE.getChildren().toArray(new LogicalType[0]));

    protected static final PagedTypeSerializer<RowData> KEY_SER =
            (PagedTypeSerializer<RowData>) KEY_SELECTOR.getProducedType().toSerializer();

    protected static final TypeSerializer<RowData> OUT_SERIALIZER =
            new RowDataSerializer(OUTPUT_TYPES);

    protected static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES, new GenericRowRecordSortComparator(0, VarCharType.STRING_TYPE));

    // ============================== Util Functions ==============================

    protected static OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            WindowAggOperator<RowData, ?> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR.getProducedType());
    }

    protected static <T> GeneratedNamespaceAggsHandleFunction<T> createGeneratedAggsHandle(
            NamespaceAggsHandleFunction<T> aggsFunction) {
        return new GeneratedNamespaceAggsHandleFunction<T>("N/A", "", new Object[0]) {
            private static final long serialVersionUID = 1L;

            @Override
            public NamespaceAggsHandleFunction<T> newInstance(ClassLoader classLoader) {
                return aggsFunction;
            }
        };
    }

    /** Get epoch mills from a timestamp string and the time zone the timestamp belongs. */
    protected static long epochMills(ZoneId shiftTimeZone, String timestampStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr);
        ZoneOffset zoneOffset = shiftTimeZone.getRules().getOffset(localDateTime);
        return localDateTime.toInstant(zoneOffset).toEpochMilli();
    }

    /**
     * This performs a {@code SUM(f1), COUNT(f1)}, where f1 is BIGINT type. The return value
     * contains {@code sum, count, window_start, window_end}.
     */
    protected abstract static class SumAndCountAggsFunctionBase<T>
            implements NamespaceAggsHandleFunction<T> {

        private static final long serialVersionUID = 1L;

        boolean openCalled;
        final AtomicInteger closeCalled = new AtomicInteger(0);

        long sum;
        boolean sumIsNull;
        long count;
        boolean countIsNull;

        protected transient JoinedRowData result;

        public void open(StateDataViewStore store) throws Exception {
            openCalled = true;
            result = new JoinedRowData();
        }

        public void setAccumulators(T window, RowData acc) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            sumIsNull = acc.isNullAt(0);
            if (!sumIsNull) {
                sum = acc.getLong(0);
            } else {
                sum = 0L;
            }

            countIsNull = acc.isNullAt(1);
            if (!countIsNull) {
                count = acc.getLong(1);
            } else {
                count = 0L;
            }
        }

        public void accumulate(RowData inputRow) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            boolean inputIsNull = inputRow.isNullAt(1);
            if (!inputIsNull) {
                sum += inputRow.getInt(1);
                count += 1;
                sumIsNull = false;
                countIsNull = false;
            }
        }

        public void retract(RowData inputRow) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            boolean inputIsNull = inputRow.isNullAt(1);
            if (!inputIsNull) {
                sum -= inputRow.getInt(1);
                count -= 1;
            }
        }

        public void merge(T window, RowData otherAcc) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            boolean sumIsNull2 = otherAcc.isNullAt(0);
            if (!sumIsNull2) {
                sum += otherAcc.getLong(0);
                sumIsNull = false;
            }
            boolean countIsNull2 = otherAcc.isNullAt(1);
            if (!countIsNull2) {
                count += otherAcc.getLong(1);
                countIsNull = false;
            }
        }

        public RowData createAccumulators() {
            if (!openCalled) {
                fail("Open was not called");
            }
            GenericRowData rowData = new GenericRowData(2);
            rowData.setField(1, 0L); // count has default 0 value
            return rowData;
        }

        public RowData getAccumulators() throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            GenericRowData row = new GenericRowData(2);
            if (!sumIsNull) {
                row.setField(0, sum);
            } else {
                row.setField(0, null);
            }
            if (!countIsNull) {
                row.setField(1, count);
            } else {
                row.setField(1, null);
            }
            return row;
        }

        public void cleanup(T window) {}

        public void close() {
            closeCalled.incrementAndGet();
        }

        @Override
        public RowData getValue(T window) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            GenericRowData row = new GenericRowData(4);
            if (!sumIsNull) {
                row.setField(0, sum);
            } else {
                row.setField(0, null);
            }
            if (!countIsNull) {
                row.setField(1, count);
            } else {
                row.setField(1, null);
            }

            row.setField(2, getWindowStart(window));
            row.setField(3, getWindowEnd(window));
            return row;
        }

        protected abstract long getWindowStart(T window);

        protected abstract long getWindowEnd(T window);
    }
}
