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

package org.apache.flink.optimizer.java;

import static org.junit.Assert.*;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

@SuppressWarnings("serial")
public class OpenIterationTest extends CompilerTestBase {

	@Test
	public void testSinkInOpenBulkIteration() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> input = env.generateSequence(1, 10);
			
			IterativeDataSet<Long> iteration = input.iterate(10);
			
			DataSet<Long> mapped = iteration.map(new IdentityMapper<Long>());
			
			mapped.print();
			
			try {
				env.createProgramPlan();
				fail("should throw an exception");
			}
			catch (InvalidProgramException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSinkInClosedBulkIteration() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> input = env.generateSequence(1, 10);
			
			IterativeDataSet<Long> iteration = input.iterate(10);
			
			DataSet<Long> mapped = iteration.map(new IdentityMapper<Long>());
			
			iteration.closeWith(mapped).output(new DiscardingOutputFormat<Long>());
			
			mapped.output(new DiscardingOutputFormat<Long>());
			
			Plan p = env.createProgramPlan();
			
			try {
				compileNoStats(p);
				fail("should throw an exception");
			}
			catch (InvalidProgramException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSinkOnSolutionSetDeltaIteration() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<Long, Long>(0L,0L));
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = input.iterateDelta(input, 10, 0);
			
			DataSet<Tuple2<Long, Long>> mapped = iteration.getSolutionSet().map(new IdentityMapper<Tuple2<Long, Long>>());
			
			mapped.print();
			
			try {
				env.createProgramPlan();
				fail("should throw an exception");
			}
			catch (InvalidProgramException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSinkOnWorksetDeltaIteration() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<Long, Long>(0L,0L));
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = input.iterateDelta(input, 10, 0);
			
			DataSet<Tuple2<Long, Long>> mapped = iteration.getWorkset().map(new IdentityMapper<Tuple2<Long, Long>>());
			
			mapped.print();
			
			try {
				env.createProgramPlan();
				fail("should throw an exception");
			}
			catch (InvalidProgramException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testOperationOnSolutionSet() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<Long, Long>(0L,0L));
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = input.iterateDelta(input, 10, 0);
			
			DataSet<Tuple2<Long, Long>> mapped = iteration.getSolutionSet().map(new IdentityMapper<Tuple2<Long, Long>>());
			
			DataSet<Tuple2<Long, Long>> joined = iteration.getWorkset().join(mapped)
												.where(0).equalTo(0).projectFirst(1).projectSecond(0);
			
			iteration.closeWith(joined, joined)
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			
			Plan p = env.createProgramPlan();
			try {
				compileNoStats(p);
				fail("should throw an exception");
			}
			catch (InvalidProgramException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
