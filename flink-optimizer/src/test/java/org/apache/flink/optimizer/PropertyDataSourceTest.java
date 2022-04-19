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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
    void checkSinglePartitionedSource1() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedSource2() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0, 1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedSource3() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0, 1, 2, 3, 4));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedSource4() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(1, 2, 3));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedSource5() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(3));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedSource6() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(2, 4));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedSource7() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0, 1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.CUSTOM_PARTITIONING);
        assertThat(gprops.getCustomPartitioner()).isNotNull();
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource1() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(0));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource2() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(0, 1));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource3() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource4() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0, 1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource5() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(4));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(4));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource6() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(2));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(0, 2));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource7() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(2));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray()))
                .isEqualTo(new FieldSet(1, 2, 3));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedGroupedSource8() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(1, 2, 3));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource1() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(1));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource2() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(1, 0));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource3() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource4() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0, 1));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource5() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(2));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray())).isEqualTo(new FieldSet(0, 2));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource6() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(2));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(new FieldSet(lprops.getGroupedFields().toArray()))
                .isEqualTo(new FieldSet(1, 2, 3));
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkSinglePartitionedOrderedSource7() {

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

        assertThat(new FieldSet(gprops.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(1, 2, 3));
        assertThat(gprops.getPartitioning()).isSameAs(PartitioningProperty.ANY_PARTITIONING);
        assertThat(lprops.getGroupedFields()).isNull();
        assertThat(lprops.getOrdering()).isNull();
    }

    @Test
    void checkCoPartitionedSources1() {

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

        assertThat(new FieldSet(gprops1.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops1.getPartitioning()).isSameAs(PartitioningProperty.CUSTOM_PARTITIONING);
        assertThat(lprops1.getGroupedFields()).isNull();
        assertThat(lprops1.getOrdering()).isNull();

        assertThat(new FieldSet(gprops2.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops2.getPartitioning()).isSameAs(PartitioningProperty.CUSTOM_PARTITIONING);
        assertThat(lprops2.getGroupedFields()).isNull();
        assertThat(lprops2.getOrdering()).isNull();

        assertThat(gprops1.getCustomPartitioner()).isEqualTo(gprops2.getCustomPartitioner());
    }

    @Test
    void checkCoPartitionedSources2() {

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

        assertThat(new FieldSet(gprops1.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops1.getPartitioning()).isSameAs(PartitioningProperty.CUSTOM_PARTITIONING);
        assertThat(lprops1.getGroupedFields()).isNull();
        assertThat(lprops1.getOrdering()).isNull();

        assertThat(new FieldSet(gprops2.getPartitioningFields().toArray()))
                .isEqualTo(new FieldSet(0));
        assertThat(gprops2.getPartitioning()).isSameAs(PartitioningProperty.CUSTOM_PARTITIONING);
        assertThat(lprops2.getGroupedFields()).isNull();
        assertThat(lprops2.getOrdering()).isNull();

        assertThat(gprops1.getCustomPartitioner()).isNotEqualTo(gprops2.getCustomPartitioner());
    }

    public static class SomePojo {
        public double doubleField;
        public int intField;
        public String stringField;
    }
}
