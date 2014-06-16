/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.util.Collector;


@SuppressWarnings({"serial", "unchecked"})
public class IterationsCompilerTest extends CompilerTestBase {

	@Test
	public void testTwoIterationsWithMapperInbetween() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> bulkResult = doBulkIteration(verticesWithInitialId, edges);
			
			DataSet<Tuple2<Long, Long>> mappedBulk = bulkResult.map(new DummyMap());
			
			DataSet<Tuple2<Long, Long>> depResult = doDeltaIteration(mappedBulk, edges);
			
			depResult.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof WorksetIterationPlanNode);
			
			WorksetIterationPlanNode wipn = (WorksetIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			assertEquals(ShipStrategyType.PARTITION_HASH, wipn.getInput1().getShipStrategy());
			assertTrue(wipn.getInput2().getTempMode().breaksPipeline());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoIterationsDirectlyChained() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> bulkResult = doBulkIteration(verticesWithInitialId, edges);
			
			DataSet<Tuple2<Long, Long>> depResult = doDeltaIteration(bulkResult, edges);
			
			depResult.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof WorksetIterationPlanNode);
			
			WorksetIterationPlanNode wipn = (WorksetIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			assertEquals(ShipStrategyType.PARTITION_HASH, wipn.getInput1().getShipStrategy());
			assertTrue(wipn.getInput2().getTempMode().breaksPipeline());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoWorksetIterationsDirectlyChained() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> firstResult = doDeltaIteration(verticesWithInitialId, edges);
			
			DataSet<Tuple2<Long, Long>> secondResult = doDeltaIteration(firstResult, edges);
			
			secondResult.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof WorksetIterationPlanNode);
			
			WorksetIterationPlanNode wipn = (WorksetIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			assertEquals(ShipStrategyType.FORWARD, wipn.getInput1().getShipStrategy());
			assertTrue(wipn.getInput2().getTempMode().breaksPipeline());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testIterationPushingWorkOut() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> input1 = env.readCsvFile("/some/file/path").types(Long.class).map(new DuplicateValue());
			
			DataSet<Tuple2<Long, Long>> input2 = env.readCsvFile("/some/file/path").types(Long.class, Long.class);
			
			doBulkIteration(input1, input2).print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof BulkIterationPlanNode);
			
			BulkIterationPlanNode bipn = (BulkIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			for (Channel c : bipn.getPartialSolutionPlanNode().getOutgoingChannels()) {
				assertEquals(ShipStrategyType.PARTITION_HASH, c.getShipStrategy());
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public static DataSet<Tuple2<Long, Long>> doBulkIteration(DataSet<Tuple2<Long, Long>> vertices, DataSet<Tuple2<Long, Long>> edges) {
		
		// open a bulk iteration
		IterativeDataSet<Tuple2<Long, Long>> iteration = vertices.iterate(100);
		
		DataSet<Tuple2<Long, Long>> changes = iteration
				.join(edges).where(0).equalTo(0).with(new Join222())
				.groupBy(0).aggregate(Aggregations.MIN, 1)
				.join(iteration).where(0).equalTo(0)
				.flatMap(new FlatMapJoin());
		
		// close the bulk iteration
		return iteration.closeWith(changes);
	}
				
		
	public static DataSet<Tuple2<Long, Long>> doDeltaIteration(DataSet<Tuple2<Long, Long>> vertices, DataSet<Tuple2<Long, Long>> edges) {

		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> depIteration = vertices.iterateDelta(vertices, 100, 0);
				
		DataSet<Tuple1<Long>> candidates = depIteration.getWorkset().join(edges).where(0).equalTo(0)
				.projectSecond(1).types(Long.class);
		
		DataSet<Tuple1<Long>> grouped = candidates.groupBy(0).reduceGroup(new Reduce101());
		
		DataSet<Tuple2<Long, Long>> candidatesDependencies = 
				grouped.join(edges).where(0).equalTo(1).projectSecond(0, 1).types(Long.class, Long.class);
		
		DataSet<Tuple2<Long, Long>> verticesWithNewComponents = 
				candidatesDependencies.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new Join222())
				.groupBy(0).aggregate(Aggregations.MIN, 1);
		
		DataSet<Tuple2<Long, Long>> updatedComponentId = 
				verticesWithNewComponents.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.flatMap(new FlatMapJoin());
		
		DataSet<Tuple2<Long, Long>> depResult = depIteration.closeWith(updatedComponentId, updatedComponentId);
		
		return depResult;
		
	}
	
	public static final class Join222 extends JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return null;
		}
	}
	
	public static final class FlatMapJoin extends FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
		
		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, Collector<Tuple2<Long, Long>> out) {}
	}
	
	public static final class DummyMap extends MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			return value;
		}
	}
	
	@ConstantFields("0")
	public static final class Reduce101 extends GroupReduceFunction<Tuple1<Long>, Tuple1<Long>> {
		
		@Override
		public void reduce(Iterator<Tuple1<Long>> values, Collector<Tuple1<Long>> out) {}
	}
	
	@ConstantFields("0")
	public static final class DuplicateValue extends MapFunction<Tuple1<Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
			return new Tuple2<Long, Long>(value.f0, value.f0);
		}
	}
}
