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

package org.apache.flink.optimizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.ReplicatingInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings({"serial", "unchecked"})
public class ReplicatingDataSourceTest extends CompilerTestBase {

    /** Tests join program with replicated data source. */
    @Test
    public void checkJoinWithReplicatedSourceInput() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.join(source2).where("*").equalTo("*").writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when join should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType joinIn1 = joinNode.getInput1().getShipStrategy();
        ShipStrategyType joinIn2 = joinNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn2);
    }

    /** Tests join program with replicated data source behind map. */
    @Test
    public void checkJoinWithReplicatedSourceInputBehindMap() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.map(new IdMap())
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when join should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType joinIn1 = joinNode.getInput1().getShipStrategy();
        ShipStrategyType joinIn2 = joinNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn2);
    }

    /** Tests join program with replicated data source behind filter. */
    @Test
    public void checkJoinWithReplicatedSourceInputBehindFilter() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.filter(new NoFilter())
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when join should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType joinIn1 = joinNode.getInput1().getShipStrategy();
        ShipStrategyType joinIn2 = joinNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn2);
    }

    /** Tests join program with replicated data source behind flatMap. */
    @Test
    public void checkJoinWithReplicatedSourceInputBehindFlatMap() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.flatMap(new IdFlatMap())
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when join should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType joinIn1 = joinNode.getInput1().getShipStrategy();
        ShipStrategyType joinIn2 = joinNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn2);
    }

    /** Tests join program with replicated data source behind map partition. */
    @Test
    public void checkJoinWithReplicatedSourceInputBehindMapPartition() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.mapPartition(new IdPMap())
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when join should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType joinIn1 = joinNode.getInput1().getShipStrategy();
        ShipStrategyType joinIn2 = joinNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn2);
    }

    /** Tests join program with replicated data source behind multiple map ops. */
    @Test
    public void checkJoinWithReplicatedSourceInputBehindMultiMaps() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.filter(new NoFilter())
                        .mapPartition(new IdPMap())
                        .flatMap(new IdFlatMap())
                        .map(new IdMap())
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when join should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode joinNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType joinIn1 = joinNode.getInput1().getShipStrategy();
        ShipStrategyType joinIn2 = joinNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, joinIn2);
    }

    /** Tests cross program with replicated data source. */
    @Test
    public void checkCrossWithReplicatedSourceInput() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.cross(source2).writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when cross should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode crossNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType crossIn1 = crossNode.getInput1().getShipStrategy();
        ShipStrategyType crossIn2 = crossNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, crossIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, crossIn2);
    }

    /** Tests cross program with replicated data source behind map and filter. */
    @Test
    public void checkCrossWithReplicatedSourceInputBehindMap() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.map(new IdMap())
                        .filter(new NoFilter())
                        .cross(source2)
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        // when cross should have forward strategy on both sides
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        DualInputPlanNode crossNode = (DualInputPlanNode) sinkNode.getPredecessor();

        ShipStrategyType crossIn1 = crossNode.getInput1().getShipStrategy();
        ShipStrategyType crossIn2 = crossNode.getInput2().getShipStrategy();

        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, crossIn1);
        Assert.assertEquals(
                "Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, crossIn2);
    }

    /**
     * Tests compiler fail for join program with replicated data source and changing parallelism.
     */
    @Test(expected = CompilerException.class)
    public void checkJoinWithReplicatedSourceInputChangingparallelism() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.join(source2)
                        .where("*")
                        .equalTo("*")
                        .setParallelism(DEFAULT_PARALLELISM + 2)
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);
    }

    /**
     * Tests compiler fail for join program with replicated data source behind map and changing
     * parallelism.
     */
    @Test(expected = CompilerException.class)
    public void checkJoinWithReplicatedSourceInputBehindMapChangingparallelism() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.map(new IdMap())
                        .setParallelism(DEFAULT_PARALLELISM + 1)
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);
    }

    /** Tests compiler fail for join program with replicated data source behind reduce. */
    @Test(expected = CompilerException.class)
    public void checkJoinWithReplicatedSourceInputBehindReduce() {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.reduce(new LastReduce())
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);
    }

    /** Tests compiler fail for join program with replicated data source behind rebalance. */
    @Test(expected = CompilerException.class)
    public void checkJoinWithReplicatedSourceInputBehindRebalance() {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        TupleTypeInfo<Tuple1<String>> typeInfo = TupleTypeInfo.getBasicTupleTypeInfo(String.class);
        ReplicatingInputFormat<Tuple1<String>, FileInputSplit> rif =
                new ReplicatingInputFormat<Tuple1<String>, FileInputSplit>(
                        new TupleCsvInputFormat<Tuple1<String>>(new Path("/some/path"), typeInfo));

        DataSet<Tuple1<String>> source1 =
                env.createInput(
                        rif, new TupleTypeInfo<Tuple1<String>>(BasicTypeInfo.STRING_TYPE_INFO));
        DataSet<Tuple1<String>> source2 = env.readCsvFile("/some/otherpath").types(String.class);

        DataSink<Tuple2<Tuple1<String>, Tuple1<String>>> out =
                source1.rebalance()
                        .join(source2)
                        .where("*")
                        .equalTo("*")
                        .writeAsText("/some/newpath");

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);
    }

    public static class IdMap<T> implements MapFunction<T, T> {

        @Override
        public T map(T value) throws Exception {
            return value;
        }
    }

    public static class NoFilter<T> implements FilterFunction<T> {

        @Override
        public boolean filter(T value) throws Exception {
            return false;
        }
    }

    public static class IdFlatMap<T> implements FlatMapFunction<T, T> {

        @Override
        public void flatMap(T value, Collector<T> out) throws Exception {
            out.collect(value);
        }
    }

    public static class IdPMap<T> implements MapPartitionFunction<T, T> {

        @Override
        public void mapPartition(Iterable<T> values, Collector<T> out) throws Exception {
            for (T v : values) {
                out.collect(v);
            }
        }
    }

    public static class LastReduce<T> implements ReduceFunction<T> {

        @Override
        public T reduce(T value1, T value2) throws Exception {
            return value2;
        }
    }
}
