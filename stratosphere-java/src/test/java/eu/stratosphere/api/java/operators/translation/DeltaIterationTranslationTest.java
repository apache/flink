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

package eu.stratosphere.api.java.operators.translation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.base.DeltaIterationBase;
import eu.stratosphere.api.common.operators.base.GenericDataSinkBase;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class DeltaIterationTranslationTest implements java.io.Serializable {

	@Test
	public void testCorrectTranslation() {
		try {
			final String JOB_NAME = "Test JobName";
			final String ITERATION_NAME = "Test Name";
			
			final String BEFORE_NEXT_WORKSET_MAP = "Some Mapper";
			
			final int[] ITERATION_KEYS = new int[] {2};
			final int NUM_ITERATIONS = 13;
			
			final int DEFAULT_DOP= 133;
			final int ITERATION_DOP = 77;
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			// ------------ construct the test program ------------------
			{
				env.setDegreeOfParallelism(DEFAULT_DOP);
				
				@SuppressWarnings("unchecked")
				DataSet<Tuple3<Double, Long, String>> initialSolutionSet = env.fromElements(new Tuple3<Double, Long, String>(3.44, 5L, "abc"));
	
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<Double, String>> initialWorkSet = env.fromElements(new Tuple2<Double, String>(1.23, "abc"));
				
				DeltaIteration<Tuple3<Double, Long, String>, Tuple2<Double, String>> iteration = initialSolutionSet.iterateDelta(initialWorkSet, NUM_ITERATIONS, ITERATION_KEYS);
				iteration.name(ITERATION_NAME).parallelism(ITERATION_DOP);
				
				// test that multiple workset consumers are supported
				DataSet<Tuple2<Double, String>> worksetSelfJoin = 
					iteration.getWorkset()
						.map(new IdentityMapper<Tuple2<Double,String>>())
						.join(iteration.getWorkset()).where(1).equalTo(1).projectFirst(0, 1).types(Double.class, String.class);
				
				DataSet<Tuple3<Double, Long, String>> joined = worksetSelfJoin.join(iteration.getSolutionSet()).where(1).equalTo(2).with(new SolutionWorksetJoin());

				DataSet<Tuple3<Double, Long, String>> result = iteration.closeWith(
						joined,
						joined.map(new NextWorksetMapper()).name(BEFORE_NEXT_WORKSET_MAP));
				
				result.print();
				result.writeAsText("/dev/null");
			}
			
			
			Plan p = env.createProgramPlan(JOB_NAME);
			
			// ------------- validate the plan ----------------
			assertEquals(JOB_NAME, p.getJobName());
			assertEquals(DEFAULT_DOP, p.getDefaultParallelism());
			
			// validate the iteration
			GenericDataSinkBase<?> sink1, sink2;
			{
				Iterator<? extends GenericDataSinkBase<?>> sinks = p.getDataSinks().iterator();
				sink1 = sinks.next();
				sink2 = sinks.next();
			}
			
			DeltaIterationBase<?, ?> iteration = (DeltaIterationBase<?, ?>) sink1.getInput();
			
			// check that multi consumer translation works for iterations
			assertEquals(iteration, sink2.getInput());
			
			// check the basic iteration properties
			assertEquals(NUM_ITERATIONS, iteration.getMaximumNumberOfIterations());
			assertArrayEquals(ITERATION_KEYS, iteration.getSolutionSetKeyFields());
			assertEquals(ITERATION_DOP, iteration.getDegreeOfParallelism());
			assertEquals(ITERATION_NAME, iteration.getName());
			
			MapOperatorBase<?, ?, ?> nextWorksetMapper = (MapOperatorBase<?, ?, ?>) iteration.getNextWorkset();
			JoinOperatorBase<?, ?, ?, ?> solutionSetJoin = (JoinOperatorBase<?, ?, ?, ?>) iteration.getSolutionSetDelta();
			JoinOperatorBase<?, ?, ?, ?> worksetSelfJoin = (JoinOperatorBase<?, ?, ?, ?>) solutionSetJoin.getFirstInput();
			MapOperatorBase<?, ?, ?> worksetMapper = (MapOperatorBase<?, ?, ?>) worksetSelfJoin.getFirstInput();
			
			assertEquals(IdentityMapper.class, worksetMapper.getUserCodeWrapper().getUserCodeClass());
			assertEquals(NextWorksetMapper.class, nextWorksetMapper.getUserCodeWrapper().getUserCodeClass());
			assertEquals(SolutionWorksetJoin.class, solutionSetJoin.getUserCodeWrapper().getUserCodeClass());
			
			assertEquals(BEFORE_NEXT_WORKSET_MAP, nextWorksetMapper.getName());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testRejectWhenSolutionSetKeysDontMatchJoin() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Double, Long, String>> initialSolutionSet = env.fromElements(new Tuple3<Double, Long, String>(3.44, 5L, "abc"));

			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Double, String>> initialWorkSet = env.fromElements(new Tuple2<Double, String>(1.23, "abc"));
			
			DeltaIteration<Tuple3<Double, Long, String>, Tuple2<Double, String>> iteration = initialSolutionSet.iterateDelta(initialWorkSet, 10, 1);
			
			try {
				iteration.getWorkset().join(iteration.getSolutionSet()).where(1).equalTo(2);
				fail("Accepted invalid program.");
			}
			catch (InvalidProgramException e) {
				// all good!
			}
			
			try {
				iteration.getSolutionSet().join(iteration.getWorkset()).where(2).equalTo(1);
				fail("Accepted invalid program.");
			}
			catch (InvalidProgramException e) {
				// all good!
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testRejectWhenSolutionSetKeysDontMatchCoGroup() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Double, Long, String>> initialSolutionSet = env.fromElements(new Tuple3<Double, Long, String>(3.44, 5L, "abc"));

			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Double, String>> initialWorkSet = env.fromElements(new Tuple2<Double, String>(1.23, "abc"));
			
			DeltaIteration<Tuple3<Double, Long, String>, Tuple2<Double, String>> iteration = initialSolutionSet.iterateDelta(initialWorkSet, 10, 1);
			
			try {
				iteration.getWorkset().coGroup(iteration.getSolutionSet()).where(1).equalTo(2).with(new SolutionWorksetCoGroup1());
				fail("Accepted invalid program.");
			}
			catch (InvalidProgramException e) {
				// all good!
			}
			
			try {
				iteration.getSolutionSet().coGroup(iteration.getWorkset()).where(2).equalTo(1).with(new SolutionWorksetCoGroup2());
				fail("Accepted invalid program.");
			}
			catch (InvalidProgramException e) {
				// all good!
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class SolutionWorksetJoin extends JoinFunction<Tuple2<Double, String>, Tuple3<Double, Long, String>, Tuple3<Double, Long, String>> {
		@Override
		public Tuple3<Double, Long, String> join(Tuple2<Double, String> first, Tuple3<Double, Long, String> second){
			return null;
		}
	}
	
	public static class NextWorksetMapper extends MapFunction<Tuple3<Double, Long, String>, Tuple2<Double, String>> {
		@Override
		public Tuple2<Double, String> map(Tuple3<Double, Long, String> value) {
			return null;
		}
	}
	
	public static class IdentityMapper<T> extends MapFunction<T, T> {

		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}
	
	public static class SolutionWorksetCoGroup1 extends CoGroupFunction<Tuple2<Double, String>, Tuple3<Double, Long, String>, Tuple3<Double, Long, String>> {

		@Override
		public void coGroup(Iterator<Tuple2<Double, String>> first, Iterator<Tuple3<Double, Long, String>> second,
				Collector<Tuple3<Double, Long, String>> out) {
		}
	}
	
	public static class SolutionWorksetCoGroup2 extends CoGroupFunction<Tuple3<Double, Long, String>, Tuple2<Double, String>, Tuple3<Double, Long, String>> {

		@Override
		public void coGroup(Iterator<Tuple3<Double, Long, String>> second, Iterator<Tuple2<Double, String>> first,
				Collector<Tuple3<Double, Long, String>> out) {
		}
	}
}
