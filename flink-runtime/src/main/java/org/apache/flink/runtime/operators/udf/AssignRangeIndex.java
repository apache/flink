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
package org.apache.flink.runtime.operators.udf;

import org.apache.flink.api.common.distributions.CommonRangeBoundaries;
import org.apache.flink.api.common.distributions.RangeBoundaries;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * This mapPartition function require a DataSet with RangeBoundaries as broadcast input, it generate
 * Tuple2 which includes range index and record itself as output.
 *
 * @param <IN> The original data type.
 */
public class AssignRangeIndex<IN> extends RichMapPartitionFunction<IN, Tuple2<Integer, IN>> {

    private TypeComparatorFactory<IN> typeComparator;

    public AssignRangeIndex(TypeComparatorFactory<IN> typeComparator) {
        this.typeComparator = typeComparator;
    }

    @Override
    public void mapPartition(Iterable<IN> values, Collector<Tuple2<Integer, IN>> out)
            throws Exception {

        List<Object> broadcastVariable =
                getRuntimeContext().getBroadcastVariable("RangeBoundaries");
        if (broadcastVariable == null || broadcastVariable.size() != 1) {
            throw new RuntimeException(
                    "AssignRangePartition require a single RangeBoundaries as broadcast input.");
        }
        Object[][] boundaryObjects = (Object[][]) broadcastVariable.get(0);
        RangeBoundaries rangeBoundaries =
                new CommonRangeBoundaries(typeComparator.createComparator(), boundaryObjects);

        Tuple2<Integer, IN> tupleWithPartitionId = new Tuple2<>();

        for (IN record : values) {
            tupleWithPartitionId.f0 = rangeBoundaries.getRangeIndex(record);
            tupleWithPartitionId.f1 = record;
            out.collect(tupleWithPartitionId);
        }
    }
}
