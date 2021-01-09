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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"serial"})
public class PropertyDataSourceTest extends CompilerTestBase {

    private List<Tuple3<Long, SomePojo, String>> tuple3PojoData =
            new ArrayList<Tuple3<Long, SomePojo, String>>();
    private TupleTypeInfo<Tuple3<Long, SomePojo, String>> tuple3PojoType =
            new TupleTypeInfo<Tuple3<Long, SomePojo, String>>(
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeExtractor.createTypeInfo(SomePojo.class),
                    BasicTypeInfo.STRING_TYPE_INFO);

    @Test
    public void checkSinglePartitionedSource1() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy(0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedSource2() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy(1, 0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(0, 1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedSource3() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("*");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(0, 1, 2, 3, 4)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedSource4() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("f1");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(1, 2, 3)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedSource5() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("f1.stringField");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(3)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedSource6() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("f1.intField; f2");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(2, 4)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedSource7() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy("byDate", 1, 0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(0, 1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING);
        Assertions.assertTrue(gprops.getCustomPartitioner() != null);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource1() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy(0).splitsGroupedBy(0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(0)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource2() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy(0).splitsGroupedBy(1, 0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(0, 1)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource3() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy(1).splitsGroupedBy(0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource4() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties().splitsPartitionedBy(0, 1).splitsGroupedBy(0);

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(0, 1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource5() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("f2").splitsGroupedBy("f2");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(4)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(4)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource6() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties()
                .splitsPartitionedBy("f1.intField")
                .splitsGroupedBy("f0; f1.intField");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(2)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(0, 2)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource7() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("f1.intField").splitsGroupedBy("f1");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(2)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(1, 2, 3)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedGroupedSource8() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties().splitsPartitionedBy("f1").splitsGroupedBy("f1.stringField");

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(1, 2, 3)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource1() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties()
                .splitsPartitionedBy(1)
                .splitsOrderedBy(new int[] {1}, new Order[] {Order.ASCENDING});

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                (new FieldSet(lprops.getGroupedFields().toArray())).equals(new FieldSet(1)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource2() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties()
                .splitsPartitionedBy(1)
                .splitsOrderedBy(new int[] {1, 0}, new Order[] {Order.ASCENDING, Order.DESCENDING});

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                (new FieldSet(lprops.getGroupedFields().toArray())).equals(new FieldSet(1, 0)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource3() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties()
                .splitsPartitionedBy(0)
                .splitsOrderedBy(new int[] {1}, new Order[] {Order.ASCENDING});

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource4() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data.getSplitDataProperties()
                .splitsPartitionedBy(0, 1)
                .splitsOrderedBy(new int[] {1}, new Order[] {Order.DESCENDING});

        data.output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(0, 1)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource5() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties()
                .splitsPartitionedBy("f1.intField")
                .splitsOrderedBy(
                        "f0; f1.intField", new Order[] {Order.ASCENDING, Order.DESCENDING});

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(2)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(0, 2)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource6() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties()
                .splitsPartitionedBy("f1.intField")
                .splitsOrderedBy("f1", new Order[] {Order.DESCENDING});

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray())).equals(new FieldSet(2)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(
                new FieldSet(lprops.getGroupedFields().toArray()).equals(new FieldSet(1, 2, 3)));
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkSinglePartitionedOrderedSource7() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple3<Long, SomePojo, String>> data =
                env.fromCollection(tuple3PojoData, tuple3PojoType);

        data.getSplitDataProperties()
                .splitsPartitionedBy("f1")
                .splitsOrderedBy("f1.stringField", new Order[] {Order.ASCENDING});

        data.output(new DiscardingOutputFormat<Tuple3<Long, SomePojo, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode = (SourcePlanNode) sinkNode.getPredecessor();

        GlobalProperties gprops = sourceNode.getGlobalProperties();
        LocalProperties lprops = sourceNode.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops.getPartitioningFields().toArray()))
                        .equals(new FieldSet(1, 2, 3)));
        Assertions.assertTrue(gprops.getPartitioning() == PartitioningProperty.ANY_PARTITIONING);
        Assertions.assertTrue(lprops.getGroupedFields() == null);
        Assertions.assertTrue(lprops.getOrdering() == null);
    }

    @Test
    public void checkCoPartitionedSources1() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data1 =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data1.getSplitDataProperties().splitsPartitionedBy("byDate", 0);

        DataSource<Tuple2<Long, String>> data2 =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data2.getSplitDataProperties().splitsPartitionedBy("byDate", 0);

        data1.union(data2).output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode1 =
                (SourcePlanNode)
                        ((NAryUnionPlanNode) sinkNode.getPredecessor())
                                .getListOfInputs()
                                .get(0)
                                .getSource();
        SourcePlanNode sourceNode2 =
                (SourcePlanNode)
                        ((NAryUnionPlanNode) sinkNode.getPredecessor())
                                .getListOfInputs()
                                .get(1)
                                .getSource();

        GlobalProperties gprops1 = sourceNode1.getGlobalProperties();
        LocalProperties lprops1 = sourceNode1.getLocalProperties();
        GlobalProperties gprops2 = sourceNode2.getGlobalProperties();
        LocalProperties lprops2 = sourceNode2.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops1.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(
                gprops1.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING);
        Assertions.assertTrue(lprops1.getGroupedFields() == null);
        Assertions.assertTrue(lprops1.getOrdering() == null);

        Assertions.assertTrue(
                (new FieldSet(gprops2.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(
                gprops2.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING);
        Assertions.assertTrue(lprops2.getGroupedFields() == null);
        Assertions.assertTrue(lprops2.getOrdering() == null);

        Assertions.assertTrue(
                gprops1.getCustomPartitioner().equals(gprops2.getCustomPartitioner()));
    }

    @Test
    public void checkCoPartitionedSources2() {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        DataSource<Tuple2<Long, String>> data1 =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data1.getSplitDataProperties().splitsPartitionedBy("byCountry", 0);

        DataSource<Tuple2<Long, String>> data2 =
                env.readCsvFile("/some/path").types(Long.class, String.class);

        data2.getSplitDataProperties().splitsPartitionedBy("byDate", 0);

        data1.union(data2).output(new DiscardingOutputFormat<Tuple2<Long, String>>());

        Plan plan = env.createProgramPlan();

        // submit the plan to the compiler
        OptimizedPlan oPlan = compileNoStats(plan);

        // check the optimized Plan
        SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
        SourcePlanNode sourceNode1 =
                (SourcePlanNode)
                        ((NAryUnionPlanNode) sinkNode.getPredecessor())
                                .getListOfInputs()
                                .get(0)
                                .getSource();
        SourcePlanNode sourceNode2 =
                (SourcePlanNode)
                        ((NAryUnionPlanNode) sinkNode.getPredecessor())
                                .getListOfInputs()
                                .get(1)
                                .getSource();

        GlobalProperties gprops1 = sourceNode1.getGlobalProperties();
        LocalProperties lprops1 = sourceNode1.getLocalProperties();
        GlobalProperties gprops2 = sourceNode2.getGlobalProperties();
        LocalProperties lprops2 = sourceNode2.getLocalProperties();

        Assertions.assertTrue(
                (new FieldSet(gprops1.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(
                gprops1.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING);
        Assertions.assertTrue(lprops1.getGroupedFields() == null);
        Assertions.assertTrue(lprops1.getOrdering() == null);

        Assertions.assertTrue(
                (new FieldSet(gprops2.getPartitioningFields().toArray())).equals(new FieldSet(0)));
        Assertions.assertTrue(
                gprops2.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING);
        Assertions.assertTrue(lprops2.getGroupedFields() == null);
        Assertions.assertTrue(lprops2.getOrdering() == null);

        Assertions.assertTrue(
                !gprops1.getCustomPartitioner().equals(gprops2.getCustomPartitioner()));
    }

    public static class SomePojo {
        public double doubleField;
        public int intField;
        public String stringField;
    }
}
