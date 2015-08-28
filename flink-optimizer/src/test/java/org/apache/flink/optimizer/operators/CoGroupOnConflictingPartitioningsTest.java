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

package org.apache.flink.optimizer.operators;

import static org.junit.Assert.*;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.testfunctions.DummyCoGroupFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

@SuppressWarnings({"serial", "unchecked"})
public class CoGroupOnConflictingPartitioningsTest extends CompilerTestBase {

	@Test
	public void testRejectCoGroupOnHashAndRangePartitioning() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
			
			Configuration cfg = new Configuration();
			cfg.setString(Optimizer.HINT_SHIP_STRATEGY_FIRST_INPUT, Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH);
			cfg.setString(Optimizer.HINT_SHIP_STRATEGY_SECOND_INPUT, Optimizer.HINT_SHIP_STRATEGY_REPARTITION_RANGE);
			
			input.coGroup(input).where(0).equalTo(0)
				.with(new DummyCoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>())
				.withParameters(cfg)
				.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());
			
			Plan p = env.createProgramPlan();
			try {
				compileNoStats(p);
				fail("This should fail with an exception");
			}
			catch (CompilerException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
