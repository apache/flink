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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.ReplicatingInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.NumberSequenceIterator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Tests for replicating DataSources. */
@RunWith(Parameterized.class)
public class ReplicatingDataSourceITCase extends MultipleProgramsTestBase {

    public ReplicatingDataSourceITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testReplicatedSourceToJoin() throws Exception {
        /*
         * Test replicated source going into join
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple1<Long>> source1 =
                env.createInput(
                                new ReplicatingInputFormat<Long, GenericInputSplit>(
                                        new ParallelIteratorInputFormat<Long>(
                                                new NumberSequenceIterator(0L, 1000L))),
                                BasicTypeInfo.LONG_TYPE_INFO)
                        .map(new ToTuple());
        DataSet<Tuple1<Long>> source2 = env.generateSequence(0L, 1000L).map(new ToTuple());

        DataSet<Tuple> pairs = source1.join(source2).where(0).equalTo(0).projectFirst(0).sum(0);

        List<Tuple> result = pairs.collect();

        String expectedResult = "(500500)";

        compareResultAsText(result, expectedResult);
    }

    @Test
    public void testReplicatedSourceToCross() throws Exception {
        /*
         * Test replicated source going into cross
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple1<Long>> source1 =
                env.createInput(
                                new ReplicatingInputFormat<Long, GenericInputSplit>(
                                        new ParallelIteratorInputFormat<Long>(
                                                new NumberSequenceIterator(0L, 1000L))),
                                BasicTypeInfo.LONG_TYPE_INFO)
                        .map(new ToTuple());
        DataSet<Tuple1<Long>> source2 = env.generateSequence(0L, 1000L).map(new ToTuple());

        DataSet<Tuple1<Long>> pairs =
                source1.cross(source2)
                        .filter(
                                new FilterFunction<Tuple2<Tuple1<Long>, Tuple1<Long>>>() {
                                    @Override
                                    public boolean filter(Tuple2<Tuple1<Long>, Tuple1<Long>> value)
                                            throws Exception {
                                        return value.f0.f0.equals(value.f1.f0);
                                    }
                                })
                        .map(
                                new MapFunction<
                                        Tuple2<Tuple1<Long>, Tuple1<Long>>, Tuple1<Long>>() {
                                    @Override
                                    public Tuple1<Long> map(
                                            Tuple2<Tuple1<Long>, Tuple1<Long>> value)
                                            throws Exception {
                                        return value.f0;
                                    }
                                })
                        .sum(0);

        List<Tuple1<Long>> result = pairs.collect();

        String expectedResult = "(500500)";

        compareResultAsText(result, expectedResult);
    }

    private static class ToTuple implements MapFunction<Long, Tuple1<Long>> {

        @Override
        public Tuple1<Long> map(Long value) throws Exception {
            return new Tuple1<Long>(value);
        }
    }
}
