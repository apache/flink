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

package org.apache.flink.test.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.summarize.BooleanColumnSummary;
import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.apache.flink.api.java.summarize.StringColumnSummary;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.types.DoubleValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/** Integration tests for {@link DataSetUtils}. */
@RunWith(Parameterized.class)
public class DataSetUtilsITCase extends MultipleProgramsTestBase {

    public DataSetUtilsITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testCountElementsPerPartition() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long expectedSize = 100L;
        DataSet<Long> numbers = env.generateSequence(0, expectedSize - 1);

        DataSet<Tuple2<Integer, Long>> ds = DataSetUtils.countElementsPerPartition(numbers);

        Assertions.assertEquals(env.getParallelism(), ds.count());
        Assertions.assertEquals(expectedSize, ds.sum(1).collect().get(0).f1.longValue());
    }

    @Test
    public void testZipWithIndex() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long expectedSize = 100L;
        DataSet<Long> numbers = env.generateSequence(0, expectedSize - 1);

        List<Tuple2<Long, Long>> result =
                new ArrayList<>(DataSetUtils.zipWithIndex(numbers).collect());

        Assertions.assertEquals(expectedSize, result.size());
        // sort result by created index
        Collections.sort(
                result,
                new Comparator<Tuple2<Long, Long>>() {
                    @Override
                    public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });
        // test if index is consecutive
        for (int i = 0; i < expectedSize; i++) {
            Assertions.assertEquals(i, result.get(i).f0.longValue());
        }
    }

    @Test
    public void testZipWithUniqueId() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long expectedSize = 100L;
        DataSet<Long> numbers = env.generateSequence(1L, expectedSize);

        DataSet<Long> ids =
                DataSetUtils.zipWithUniqueId(numbers)
                        .map(
                                new MapFunction<Tuple2<Long, Long>, Long>() {
                                    @Override
                                    public Long map(Tuple2<Long, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                });

        Set<Long> result = new HashSet<>(ids.collect());

        Assertions.assertEquals(expectedSize, result.size());
    }

    @Test
    public void testIntegerDataSetChecksumHashCode() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> ds = CollectionDataSets.getIntegerDataSet(env);

        Utils.ChecksumHashCode checksum = DataSetUtils.checksumHashCode(ds);
        Assertions.assertEquals(checksum.getCount(), 15);
        Assertions.assertEquals(checksum.getChecksum(), 55);
    }

    @Test
    public void testSummarize() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple8<Short, Integer, Long, Float, Double, String, Boolean, DoubleValue>> data =
                new ArrayList<>();
        data.add(
                new Tuple8<>(
                        (short) 1, 1, 100L, 0.1f, 1.012376, "hello", false, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 2, 2, 1000L, 0.2f, 2.003453, "hello", true, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 4,
                        10,
                        10000L,
                        0.2f,
                        75.00005,
                        "null",
                        true,
                        new DoubleValue(50.0)));
        data.add(new Tuple8<>((short) 10, 4, 100L, 0.9f, 79.5, "", true, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 5, 5, 1000L, 0.2f, 10.0000001, "a", false, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 6,
                        6,
                        10L,
                        0.1f,
                        0.0000000000023,
                        "",
                        true,
                        new DoubleValue(100.0)));
        data.add(
                new Tuple8<>(
                        (short) 7,
                        7,
                        1L,
                        0.2f,
                        Double.POSITIVE_INFINITY,
                        "abcdefghijklmnop",
                        true,
                        new DoubleValue(100.0)));
        data.add(
                new Tuple8<>(
                        (short) 8,
                        8,
                        -100L,
                        0.001f,
                        Double.NaN,
                        "abcdefghi",
                        true,
                        new DoubleValue(100.0)));

        Collections.shuffle(data);

        DataSet<Tuple8<Short, Integer, Long, Float, Double, String, Boolean, DoubleValue>> ds =
                env.fromCollection(data);

        // call method under test
        Tuple results = DataSetUtils.summarize(ds);

        Assertions.assertEquals(8, results.getArity());

        NumericColumnSummary<Short> col0Summary = results.getField(0);
        Assertions.assertEquals(8, col0Summary.getNonMissingCount());
        Assertions.assertEquals(1, col0Summary.getMin().shortValue());
        Assertions.assertEquals(10, col0Summary.getMax().shortValue());
        Assertions.assertEquals(5.375, col0Summary.getMean().doubleValue(), 0.0);

        NumericColumnSummary<Integer> col1Summary = results.getField(1);
        Assertions.assertEquals(1, col1Summary.getMin().intValue());
        Assertions.assertEquals(10, col1Summary.getMax().intValue());
        Assertions.assertEquals(5.375, col1Summary.getMean().doubleValue(), 0.0);

        NumericColumnSummary<Long> col2Summary = results.getField(2);
        Assertions.assertEquals(-100L, col2Summary.getMin().longValue());
        Assertions.assertEquals(10000L, col2Summary.getMax().longValue());

        NumericColumnSummary<Float> col3Summary = results.getField(3);
        Assertions.assertEquals(8, col3Summary.getTotalCount());
        Assertions.assertEquals(0.001000, col3Summary.getMin().doubleValue(), 0.0000001);
        Assertions.assertEquals(0.89999999, col3Summary.getMax().doubleValue(), 0.0000001);
        Assertions.assertEquals(
                0.2376249988883501, col3Summary.getMean().doubleValue(), 0.000000000001);
        Assertions.assertEquals(
                0.0768965488108089, col3Summary.getVariance().doubleValue(), 0.00000001);
        Assertions.assertEquals(
                0.27730226975415995,
                col3Summary.getStandardDeviation().doubleValue(),
                0.000000000001);

        NumericColumnSummary<Double> col4Summary = results.getField(4);
        Assertions.assertEquals(6, col4Summary.getNonMissingCount());
        Assertions.assertEquals(2, col4Summary.getMissingCount());
        Assertions.assertEquals(0.0000000000023, col4Summary.getMin().doubleValue(), 0.0);
        Assertions.assertEquals(79.5, col4Summary.getMax().doubleValue(), 0.000000000001);

        StringColumnSummary col5Summary = results.getField(5);
        Assertions.assertEquals(8, col5Summary.getTotalCount());
        Assertions.assertEquals(0, col5Summary.getNullCount());
        Assertions.assertEquals(8, col5Summary.getNonNullCount());
        Assertions.assertEquals(2, col5Summary.getEmptyCount());
        Assertions.assertEquals(0, col5Summary.getMinLength().intValue());
        Assertions.assertEquals(16, col5Summary.getMaxLength().intValue());
        Assertions.assertEquals(5.0, col5Summary.getMeanLength().doubleValue(), 0.0001);

        BooleanColumnSummary col6Summary = results.getField(6);
        Assertions.assertEquals(8, col6Summary.getTotalCount());
        Assertions.assertEquals(2, col6Summary.getFalseCount());
        Assertions.assertEquals(6, col6Summary.getTrueCount());
        Assertions.assertEquals(0, col6Summary.getNullCount());

        NumericColumnSummary<Double> col7Summary = results.getField(7);
        Assertions.assertEquals(100.0, col7Summary.getMax().doubleValue(), 0.00001);
        Assertions.assertEquals(50.0, col7Summary.getMin().doubleValue(), 0.00001);
    }
}
