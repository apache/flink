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

package org.apache.flink.test.preaggregatedaccumulators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.test.preaggregatedaccumulators.utils.PreAggregatedAccumulatorProgram;

import java.util.Map;

/**
 * Tests the basic functionality of pre-aggregated accumulators with the PRODUCER_WAIT_CONSUMER_FINISH mode.
 * In this mode, the producer tasks do not commit the accumulator till the consumer tasks finish.
 */
public class PreAggregatedAccumulatorWithFailQueryITCase extends StreamingProgramTestBase {
	private Map<Integer, Integer> numberReceived;

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.numberReceived = PreAggregatedAccumulatorProgram.executeJob(
			PreAggregatedAccumulatorProgram.ExecutionMode.PRODUCER_WAIT_CONSUMER_FINISH, env);
	}

	@Override
	protected void postSubmit() throws Exception {
		PreAggregatedAccumulatorProgram.assertResultConsistent(numberReceived);
	}
}
