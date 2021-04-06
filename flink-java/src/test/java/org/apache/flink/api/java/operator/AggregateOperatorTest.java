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

package org.apache.flink.api.java.operator;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.aggregation.UnsupportedAggregationTypeException;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link DataSet#aggregate(Aggregations, int)}. */
public class AggregateOperatorTest {

    // TUPLE DATA

    private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData =
            new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    // LONG DATA

    private final List<Long> emptyLongData = new ArrayList<Long>();

    @Test
    public void testFieldsAggregate() {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                env.fromCollection(emptyTupleData, tupleTypeInfo);

        // should work
        try {
            tupleDs.aggregate(Aggregations.SUM, 1);
        } catch (Exception e) {
            Assert.fail();
        }

        // should not work: index out of bounds
        try {
            tupleDs.aggregate(Aggregations.SUM, 10);
            Assert.fail();
        } catch (IllegalArgumentException iae) {
            // we're good here
        } catch (Exception e) {
            Assert.fail();
        }

        // should not work: not applied to tuple dataset
        DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
        try {
            longDs.aggregate(Aggregations.MIN, 1);
            Assert.fail();
        } catch (InvalidProgramException uoe) {
            // we're good here
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testAggregationTypes() {
        try {
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs =
                    env.fromCollection(emptyTupleData, tupleTypeInfo);

            // should work: multiple aggregates
            tupleDs.aggregate(Aggregations.SUM, 0).and(Aggregations.MIN, 4);

            // should work: nested aggregates
            tupleDs.aggregate(Aggregations.MIN, 2).aggregate(Aggregations.SUM, 1);

            // should not work: average on string
            try {
                tupleDs.aggregate(Aggregations.SUM, 2);
                Assert.fail();
            } catch (UnsupportedAggregationTypeException iae) {
                // we're good here
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
