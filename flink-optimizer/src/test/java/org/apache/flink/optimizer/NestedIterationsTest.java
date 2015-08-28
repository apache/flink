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

import static org.junit.Assert.*;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.optimizer.testfunctions.IdentityKeyExtractor;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

@SuppressWarnings({"serial", "unchecked"})
public class NestedIterationsTest extends CompilerTestBase {

	@Test
	public void testRejectNestedBulkIterations() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> data = env.generateSequence(1, 100);
			
			IterativeDataSet<Long> outerIteration = data.iterate(100);
			
			IterativeDataSet<Long> innerIteration = outerIteration.map(new IdentityMapper<Long>()).iterate(100);
			
			DataSet<Long> innerResult = innerIteration.closeWith(innerIteration.map(new IdentityMapper<Long>()));
			
			DataSet<Long> outerResult = outerIteration.closeWith(innerResult.map(new IdentityMapper<Long>()));
			
			outerResult.output(new DiscardingOutputFormat<Long>());
			
			Plan p = env.createProgramPlan();
			
			try {
				compileNoStats(p);
			}
			catch (CompilerException e) {
				assertTrue(e.getMessage().toLowerCase().indexOf("nested iterations") != -1);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testRejectNestedWorksetIterations() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> data = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> outerIteration = data.iterateDelta(data, 100, 0);
			
			DataSet<Tuple2<Long, Long>> inOuter = outerIteration.getWorkset().map(new IdentityMapper<Tuple2<Long, Long>>());
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> innerIteration = inOuter.iterateDelta(inOuter, 100, 0);
			
			DataSet<Tuple2<Long, Long>> inInner = innerIteration.getWorkset().map(new IdentityMapper<Tuple2<Long, Long>>());
			
			DataSet<Tuple2<Long, Long>> innerResult = innerIteration.closeWith(inInner, inInner).map(new IdentityMapper<Tuple2<Long,Long>>());
			
			DataSet<Tuple2<Long, Long>> outerResult = outerIteration.closeWith(innerResult, innerResult);
			
			outerResult.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			
			Plan p = env.createProgramPlan();
			
			try {
				compileNoStats(p);
			}
			catch (CompilerException e) {
				assertTrue(e.getMessage().toLowerCase().indexOf("nested iterations") != -1);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBulkIterationInClosure() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> data1 = env.generateSequence(1, 100);
			DataSet<Long> data2 = env.generateSequence(1, 100);
			
			IterativeDataSet<Long> firstIteration = data1.iterate(100);
			
			DataSet<Long> firstResult = firstIteration.closeWith(firstIteration.map(new IdentityMapper<Long>()));
			
			
			IterativeDataSet<Long> mainIteration = data2.map(new IdentityMapper<Long>()).iterate(100);
			
			DataSet<Long> joined = mainIteration.join(firstResult)
					.where(new IdentityKeyExtractor<Long>()).equalTo(new IdentityKeyExtractor<Long>())
					.with(new DummyFlatJoinFunction<Long>());
			
			DataSet<Long> mainResult = mainIteration.closeWith(joined);
			
			mainResult.output(new DiscardingOutputFormat<Long>());
			
			Plan p = env.createProgramPlan();
			
			// optimizer should be able to translate this
			OptimizedPlan op = compileNoStats(p);
			
			// job graph generator should be able to translate this
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeltaIterationInClosure() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> data1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
			DataSet<Tuple2<Long, Long>> data2 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> firstIteration = data1.iterateDelta(data1, 100, 0);
			
			DataSet<Tuple2<Long, Long>> inFirst = firstIteration.getWorkset().map(new IdentityMapper<Tuple2<Long, Long>>());
			
			DataSet<Tuple2<Long, Long>> firstResult = firstIteration.closeWith(inFirst, inFirst).map(new IdentityMapper<Tuple2<Long,Long>>());
			
			
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> mainIteration = data2.iterateDelta(data2, 100, 0);
			
			DataSet<Tuple2<Long, Long>> joined = mainIteration.getWorkset().join(firstResult).where(0).equalTo(0)
							.projectFirst(0).projectSecond(0);
			
			DataSet<Tuple2<Long, Long>> mainResult = mainIteration.closeWith(joined, joined);
			
			mainResult.output(new DiscardingOutputFormat<Tuple2<Long,Long>>());
			
			Plan p = env.createProgramPlan();
			
			// optimizer should be able to translate this
			OptimizedPlan op = compileNoStats(p);
			
			// job graph generator should be able to translate this
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
