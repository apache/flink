/**
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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class PartitioningReusageTest extends CompilerTestBase {

	@Test
	public void noPreviousPartitioningJoin1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.join(set2, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
					.where(0,1).equalTo(0,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);

	}

	@Test
	public void noPreviousPartitioningJoin2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.join(set2, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);

	}

	@Test
	public void reuseSinglePartitioningJoin1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.join(set2, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(0,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);

	}

	@Test
	public void reuseSinglePartitioningJoin2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.join(set2, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseSinglePartitioningJoin3() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.join(set2.partitionByHash(2, 1)
							.map(new MockMapper())
							.withForwardedFields("2;1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseSinglePartitioningJoin4() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0)
				.map(new MockMapper()).withForwardedFields("0")
				.join(set2, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseSinglePartitioningJoin5() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.join(set2.partitionByHash(2)
							.map(new MockMapper())
							.withForwardedFields("2"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseBothPartitioningJoin1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.join(set2.partitionByHash(0,1)
							.map(new MockMapper())
							.withForwardedFields("0;1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(0,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}


	@Test
	public void reuseBothPartitioningJoin2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.join(set2.partitionByHash(1,2)
								.map(new MockMapper())
								.withForwardedFields("1;2"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseBothPartitioningJoin3() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0)
				.map(new MockMapper()).withForwardedFields("0")
				.join(set2.partitionByHash(2,1)
								.map(new MockMapper())
								.withForwardedFields("2;1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,1).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseBothPartitioningJoin4() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0,2)
				.map(new MockMapper()).withForwardedFields("0;2")
				.join(set2.partitionByHash(1)
								.map(new MockMapper())
								.withForwardedFields("1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,2).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseBothPartitioningJoin5() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(2)
				.map(new MockMapper()).withForwardedFields("2")
				.join(set2.partitionByHash(1)
								.map(new MockMapper())
								.withForwardedFields("1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,2).equalTo(2,1).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseBothPartitioningJoin6() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(0)
				.map(new MockMapper()).withForwardedFields("0")
				.join(set2.partitionByHash(1)
								.map(new MockMapper())
								.withForwardedFields("1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,2).equalTo(1,2).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}

	@Test
	public void reuseBothPartitioningJoin7() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> joined = set1
				.partitionByHash(2)
				.map(new MockMapper()).withForwardedFields("2")
				.join(set2.partitionByHash(1)
								.map(new MockMapper())
								.withForwardedFields("1"),
						JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
				.where(0,2).equalTo(1,2).with(new MockJoin());

		joined.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode join = (DualInputPlanNode)sink.getInput().getSource();

		checkValidJoinInputProperties(join);
	}


	@Test
	public void noPreviousPartitioningCoGroup1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.coGroup(set2)
				.where(0,1).equalTo(0,1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);

	}

	@Test
	public void noPreviousPartitioningCoGroup2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.coGroup(set2)
				.where(0,1).equalTo(2,1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);

	}

	@Test
	public void reuseSinglePartitioningCoGroup1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.coGroup(set2)
				.where(0,1).equalTo(0,1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);

	}

	@Test
	public void reuseSinglePartitioningCoGroup2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.coGroup(set2)
				.where(0,1).equalTo(2,1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseSinglePartitioningCoGroup3() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.coGroup(set2.partitionByHash(2, 1)
								.map(new MockMapper())
								.withForwardedFields("2;1"))
				.where(0,1).equalTo(2, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseSinglePartitioningCoGroup4() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0)
				.map(new MockMapper()).withForwardedFields("0")
				.coGroup(set2)
				.where(0, 1).equalTo(2, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseSinglePartitioningCoGroup5() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.coGroup(set2.partitionByHash(2)
								.map(new MockMapper())
								.withForwardedFields("2"))
				.where(0,1).equalTo(2,1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseBothPartitioningCoGroup1() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.coGroup(set2.partitionByHash(0, 1)
						.map(new MockMapper())
						.withForwardedFields("0;1"))
				.where(0, 1).equalTo(0, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}


	@Test
	public void reuseBothPartitioningCoGroup2() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0,1)
				.map(new MockMapper()).withForwardedFields("0;1")
				.coGroup(set2.partitionByHash(1, 2)
						.map(new MockMapper())
						.withForwardedFields("1;2"))
				.where(0, 1).equalTo(2, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseBothPartitioningCoGroup3() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0)
				.map(new MockMapper()).withForwardedFields("0")
				.coGroup(set2.partitionByHash(2, 1)
						.map(new MockMapper())
						.withForwardedFields("2;1"))
				.where(0, 1).equalTo(2, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseBothPartitioningCoGroup4() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(0,2)
				.map(new MockMapper()).withForwardedFields("0;2")
				.coGroup(set2.partitionByHash(1)
						.map(new MockMapper())
						.withForwardedFields("1"))
				.where(0, 2).equalTo(2, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseBothPartitioningCoGroup5() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(2)
				.map(new MockMapper()).withForwardedFields("2")
				.coGroup(set2.partitionByHash(1)
						.map(new MockMapper())
						.withForwardedFields("1"))
				.where(0, 2).equalTo(2, 1).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseBothPartitioningCoGroup6() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(2)
				.map(new MockMapper()).withForwardedFields("2")
				.coGroup(set2.partitionByHash(2)
						.map(new MockMapper())
						.withForwardedFields("2"))
				.where(0, 2).equalTo(1, 2).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}

	@Test
	public void reuseBothPartitioningCoGroup7() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> set2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> coGrouped = set1
				.partitionByHash(2)
				.map(new MockMapper()).withForwardedFields("2")
				.coGroup(set2.partitionByHash(1)
						.map(new MockMapper())
						.withForwardedFields("1"))
				.where(0, 2).equalTo(1, 2).with(new MockCoGroup());

		coGrouped.output(new DiscardingOutputFormat<Tuple3<Integer, Integer, Integer>>());
		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		SinkPlanNode sink = oPlan.getDataSinks().iterator().next();
		DualInputPlanNode coGroup= (DualInputPlanNode)sink.getInput().getSource();

		checkValidCoGroupInputProperties(coGroup);
	}



	private void checkValidJoinInputProperties(DualInputPlanNode join) {

		GlobalProperties inProps1 = join.getInput1().getGlobalProperties();
		GlobalProperties inProps2 = join.getInput2().getGlobalProperties();

		if(inProps1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
				inProps2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED) {

			// check that both inputs are hash partitioned on the same fields
			FieldList pFields1 = inProps1.getPartitioningFields();
			FieldList pFields2 = inProps2.getPartitioningFields();

			assertTrue("Inputs are not the same number of fields. Input 1: "+pFields1+", Input 2: "+pFields2,
					pFields1.size() == pFields2.size());

			FieldList reqPFields1 = join.getKeysForInput1();
			FieldList reqPFields2 = join.getKeysForInput2();

			for(int i=0; i<pFields1.size(); i++) {

				// get fields
				int f1 = pFields1.get(i);
				int f2 = pFields2.get(i);

				// check that field positions in original key field list are identical
				int pos1 = getPosInFieldList(f1, reqPFields1);
				int pos2 = getPosInFieldList(f2, reqPFields2);

				if(pos1 < 0) {
					fail("Input 1 is partitioned on field "+f1+" which is not contained in the key set "+reqPFields1);
				}
				if(pos2 < 0) {
					fail("Input 2 is partitioned on field "+f2+" which is not contained in the key set "+reqPFields2);
				}
				if(pos1 != pos2) {
					fail("Inputs are not partitioned on the same key fields");
				}
			}

		}
		else if(inProps1.getPartitioning() == PartitioningProperty.FULL_REPLICATION &&
				inProps2.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED) {
			// we are good. No need to check for fields
		}
		else if(inProps1.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED &&
				inProps2.getPartitioning() == PartitioningProperty.FULL_REPLICATION) {
			// we are good. No need to check for fields
		}
		else {
			throw new UnsupportedOperationException("This method has only been implemented to check for hash partitioned coGroupinputs");
		}

	}

	private void checkValidCoGroupInputProperties(DualInputPlanNode coGroup) {

		GlobalProperties inProps1 = coGroup.getInput1().getGlobalProperties();
		GlobalProperties inProps2 = coGroup.getInput2().getGlobalProperties();

		if(inProps1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
				inProps2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED) {

			// check that both inputs are hash partitioned on the same fields
			FieldList pFields1 = inProps1.getPartitioningFields();
			FieldList pFields2 = inProps2.getPartitioningFields();

			assertTrue("Inputs are not the same number of fields. Input 1: "+pFields1+", Input 2: "+pFields2,
					pFields1.size() == pFields2.size());

			FieldList reqPFields1 = coGroup.getKeysForInput1();
			FieldList reqPFields2 = coGroup.getKeysForInput2();

			for(int i=0; i<pFields1.size(); i++) {

				// get fields
				int f1 = pFields1.get(i);
				int f2 = pFields2.get(i);

				// check that field positions in original key field list are identical
				int pos1 = getPosInFieldList(f1, reqPFields1);
				int pos2 = getPosInFieldList(f2, reqPFields2);

				if(pos1 < 0) {
					fail("Input 1 is partitioned on field "+f1+" which is not contained in the key set "+reqPFields1);
				}
				if(pos2 < 0) {
					fail("Input 2 is partitioned on field "+f2+" which is not contained in the key set "+reqPFields2);
				}
				if(pos1 != pos2) {
					fail("Inputs are not partitioned on the same key fields");
				}
			}

		}
		else {
			throw new UnsupportedOperationException("This method has only been implemented to check for hash partitioned coGroup inputs");
		}

	}

	private int getPosInFieldList(int field, FieldList list) {

		int pos;
		for(pos=0; pos<list.size(); pos++) {
			if(field == list.get(pos)) {
				break;
			}
		}
		if(pos == list.size()) {
			return -1;
		} else {
			return pos;
		}

	}



	public static class MockMapper implements MapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> value) throws Exception {
			return null;
		}
	}

	public static class MockJoin implements JoinFunction<Tuple3<Integer, Integer, Integer>,
			Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

		@Override
		public Tuple3<Integer, Integer, Integer> join(Tuple3<Integer, Integer, Integer> first, Tuple3<Integer, Integer, Integer> second) throws Exception {
			return null;
		}
	}

	public static class MockCoGroup implements CoGroupFunction<Tuple3<Integer, Integer, Integer>,
				Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

		@Override
		public void coGroup(Iterable<Tuple3<Integer, Integer, Integer>> first, Iterable<Tuple3<Integer, Integer, Integer>> second,
							Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

		}
	}

}

