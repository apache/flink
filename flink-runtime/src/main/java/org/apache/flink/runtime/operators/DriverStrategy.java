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

package org.apache.flink.runtime.operators;

import org.apache.flink.runtime.operators.chaining.ChainedAllReduceDriver;
import org.apache.flink.runtime.operators.chaining.ChainedDriver;
import org.apache.flink.runtime.operators.chaining.ChainedFlatMapDriver;
import org.apache.flink.runtime.operators.chaining.ChainedMapDriver;
import org.apache.flink.runtime.operators.chaining.ChainedReduceCombineDriver;
import org.apache.flink.runtime.operators.chaining.SynchronousChainedCombineDriver;

import static org.apache.flink.runtime.operators.DamBehavior.FULL_DAM;
import static org.apache.flink.runtime.operators.DamBehavior.MATERIALIZING;
import static org.apache.flink.runtime.operators.DamBehavior.PIPELINED;

/** Enumeration of all available operator strategies. */
public enum DriverStrategy {
    // no local strategy, as for sources and sinks
    NONE(null, null, PIPELINED, 0),
    // a unary no-op operator
    UNARY_NO_OP(NoOpDriver.class, NoOpChainedDriver.class, PIPELINED, PIPELINED, 0),
    // a binary no-op operator. non implementation available
    BINARY_NO_OP(null, null, PIPELINED, PIPELINED, 0),

    // the proper mapper
    MAP(MapDriver.class, ChainedMapDriver.class, PIPELINED, 0),

    // the proper map partition
    MAP_PARTITION(MapPartitionDriver.class, null, PIPELINED, 0),

    // the flat mapper
    FLAT_MAP(FlatMapDriver.class, ChainedFlatMapDriver.class, PIPELINED, 0),

    // group everything together into one group and apply the Reduce function
    ALL_REDUCE(AllReduceDriver.class, ChainedAllReduceDriver.class, PIPELINED, 0),
    // group everything together into one group and apply the GroupReduce function
    ALL_GROUP_REDUCE(AllGroupReduceDriver.class, null, PIPELINED, 0),
    // group everything together into one group and apply the GroupReduce's combine function
    ALL_GROUP_REDUCE_COMBINE(AllGroupReduceDriver.class, null, PIPELINED, 0),

    // grouping the inputs and apply the Reduce Function
    SORTED_REDUCE(ReduceDriver.class, null, PIPELINED, 1),
    // sorted partial reduce is a combiner for the Reduce. same function, but potentially not fully
    // sorted
    SORTED_PARTIAL_REDUCE(
            ReduceCombineDriver.class, ChainedReduceCombineDriver.class, MATERIALIZING, 1),

    // hashed partial reduce is a combiner for the Reduce
    HASHED_PARTIAL_REDUCE(
            ReduceCombineDriver.class, ChainedReduceCombineDriver.class, MATERIALIZING, 1),

    // grouping the inputs and apply the GroupReduce function
    SORTED_GROUP_REDUCE(GroupReduceDriver.class, null, PIPELINED, 1),
    // partially grouping inputs (best effort resulting possibly in duplicates --> combiner)
    SORTED_GROUP_COMBINE(
            GroupReduceCombineDriver.class,
            SynchronousChainedCombineDriver.class,
            MATERIALIZING,
            2),

    // group combine on all inputs within a partition (without grouping)
    ALL_GROUP_COMBINE(AllGroupCombineDriver.class, null, PIPELINED, 0),

    // both inputs are merged, but materialized to the side for block-nested-loop-join among values
    // with equal key
    INNER_MERGE(JoinDriver.class, null, MATERIALIZING, MATERIALIZING, 2),
    LEFT_OUTER_MERGE(LeftOuterJoinDriver.class, null, MATERIALIZING, MATERIALIZING, 2),
    RIGHT_OUTER_MERGE(RightOuterJoinDriver.class, null, MATERIALIZING, MATERIALIZING, 2),
    FULL_OUTER_MERGE(FullOuterJoinDriver.class, null, MATERIALIZING, MATERIALIZING, 2),

    // co-grouping inputs
    CO_GROUP(CoGroupDriver.class, null, PIPELINED, PIPELINED, 2),
    // python-cogroup
    CO_GROUP_RAW(CoGroupRawDriver.class, null, PIPELINED, PIPELINED, 0),

    // the first input is build side, the second side is probe side of a hybrid hash table
    HYBRIDHASH_BUILD_FIRST(JoinDriver.class, null, FULL_DAM, MATERIALIZING, 2),
    // the second input is build side, the first side is probe side of a hybrid hash table
    HYBRIDHASH_BUILD_SECOND(JoinDriver.class, null, MATERIALIZING, FULL_DAM, 2),
    // a cached variant of HYBRIDHASH_BUILD_FIRST, that can only be used inside of iterations
    HYBRIDHASH_BUILD_FIRST_CACHED(
            BuildFirstCachedJoinDriver.class, null, FULL_DAM, MATERIALIZING, 2),
    //  cached variant of HYBRIDHASH_BUILD_SECOND, that can only be used inside of iterations
    HYBRIDHASH_BUILD_SECOND_CACHED(
            BuildSecondCachedJoinDriver.class, null, MATERIALIZING, FULL_DAM, 2),

    // right outer join, the first input is build side, the second input is probe side of a hybrid
    // hash table.
    RIGHT_HYBRIDHASH_BUILD_FIRST(RightOuterJoinDriver.class, null, FULL_DAM, MATERIALIZING, 2),
    // right outer join, the first input is probe side, the second input is build side of a hybrid
    // hash table.
    RIGHT_HYBRIDHASH_BUILD_SECOND(RightOuterJoinDriver.class, null, FULL_DAM, MATERIALIZING, 2),
    // left outer join, the first input is build side, the second input is probe side of a hybrid
    // hash table.
    LEFT_HYBRIDHASH_BUILD_FIRST(LeftOuterJoinDriver.class, null, MATERIALIZING, FULL_DAM, 2),
    // left outer join, the first input is probe side, the second input is build side of a hybrid
    // hash table.
    LEFT_HYBRIDHASH_BUILD_SECOND(LeftOuterJoinDriver.class, null, MATERIALIZING, FULL_DAM, 2),
    // full outer join, the first input is build side, the second input is the probe side of a
    // hybrid hash table.
    FULL_OUTER_HYBRIDHASH_BUILD_FIRST(FullOuterJoinDriver.class, null, FULL_DAM, MATERIALIZING, 2),
    // full outer join, the first input is probe side, the second input is the build side of a
    // hybrid hash table.
    FULL_OUTER_HYBRIDHASH_BUILD_SECOND(FullOuterJoinDriver.class, null, MATERIALIZING, FULL_DAM, 2),

    // the second input is inner loop, the first input is outer loop and block-wise processed
    NESTEDLOOP_BLOCKED_OUTER_FIRST(CrossDriver.class, null, MATERIALIZING, FULL_DAM, 0),
    // the first input is inner loop, the second input is outer loop and block-wise processed
    NESTEDLOOP_BLOCKED_OUTER_SECOND(CrossDriver.class, null, FULL_DAM, MATERIALIZING, 0),
    // the second input is inner loop, the first input is outer loop and stream-processed
    NESTEDLOOP_STREAMED_OUTER_FIRST(CrossDriver.class, null, PIPELINED, FULL_DAM, 0),
    // the first input is inner loop, the second input is outer loop and stream-processed
    NESTEDLOOP_STREAMED_OUTER_SECOND(CrossDriver.class, null, FULL_DAM, PIPELINED, 0),

    // union utility op. unions happen implicitly on the network layer (in the readers) when
    // bundling streams
    UNION(null, null, PIPELINED, PIPELINED, 0),
    // explicit binary union between a streamed and a cached input
    UNION_WITH_CACHED(UnionWithTempOperator.class, null, FULL_DAM, PIPELINED, 0),

    // some enumeration constants to mark sources and sinks
    SOURCE(null, null, PIPELINED, 0),
    SINK(null, null, PIPELINED, 0);

    // --------------------------------------------------------------------------------------------

    private final Class<? extends Driver<?, ?>> driverClass;

    private final Class<? extends ChainedDriver<?, ?>> pushChainDriver;

    private final DamBehavior dam1;
    private final DamBehavior dam2;

    private final int numInputs;

    private final int numRequiredComparators;

    @SuppressWarnings("unchecked")
    private DriverStrategy(
            @SuppressWarnings("rawtypes") Class<? extends Driver> driverClass,
            @SuppressWarnings("rawtypes") Class<? extends ChainedDriver> pushChainDriverClass,
            DamBehavior dam,
            int numComparator) {
        this.driverClass = (Class<? extends Driver<?, ?>>) driverClass;
        this.pushChainDriver = (Class<? extends ChainedDriver<?, ?>>) pushChainDriverClass;
        this.numInputs = 1;
        this.dam1 = dam;
        this.dam2 = null;
        this.numRequiredComparators = numComparator;
    }

    @SuppressWarnings("unchecked")
    private DriverStrategy(
            @SuppressWarnings("rawtypes") Class<? extends Driver> driverClass,
            @SuppressWarnings("rawtypes") Class<? extends ChainedDriver> pushChainDriverClass,
            DamBehavior firstDam,
            DamBehavior secondDam,
            int numComparator) {
        this.driverClass = (Class<? extends Driver<?, ?>>) driverClass;
        this.pushChainDriver = (Class<? extends ChainedDriver<?, ?>>) pushChainDriverClass;
        this.numInputs = 2;
        this.dam1 = firstDam;
        this.dam2 = secondDam;
        this.numRequiredComparators = numComparator;
    }

    // --------------------------------------------------------------------------------------------

    public Class<? extends Driver<?, ?>> getDriverClass() {
        return this.driverClass;
    }

    public Class<? extends ChainedDriver<?, ?>> getPushChainDriverClass() {
        return this.pushChainDriver;
    }

    public int getNumInputs() {
        return this.numInputs;
    }

    public DamBehavior firstDam() {
        return this.dam1;
    }

    public DamBehavior secondDam() {
        if (this.numInputs == 2) {
            return this.dam2;
        } else {
            throw new IllegalArgumentException("The given strategy does not work on two inputs.");
        }
    }

    public DamBehavior damOnInput(int num) {
        if (num < this.numInputs) {
            if (num == 0) {
                return this.dam1;
            } else if (num == 1) {
                return this.dam2;
            }
        }
        throw new IllegalArgumentException();
    }

    public boolean isMaterializing() {
        return this.dam1.isMaterializing() || (this.dam2 != null && this.dam2.isMaterializing());
    }

    public int getNumRequiredComparators() {
        return this.numRequiredComparators;
    }
}
