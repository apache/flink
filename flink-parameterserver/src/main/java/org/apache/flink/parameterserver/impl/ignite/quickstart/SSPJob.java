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

package org.apache.flink.parameterserver.impl.ignite.quickstart;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.parameterserver.impl.ignite.ParameterElementImpl;
import org.apache.flink.parameterserver.impl.ignite.functions.RichMapFunctionWithParameterServer;

import java.util.Arrays;
import java.util.List;

/*
 * Example job illustrating the use of Stale Synchronous Parallel iterations
 * along with a parameter server.
 * This sample job uses the Apache Ignite implementation
 */
public class SSPJob {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		/**
		 * All this job does is take values from an array, increment them in an iterative way
		 * and store them in the parameter server with the workerID as key
		 */
		List<Integer> mockValues = Arrays.asList(1, 2, 3, 4, 5);
		DataSet<Integer> set = env.fromCollection(mockValues);
		IterativeDataSet<Integer> loop = set.iterateWithSSP(10,3);
		DataSet<Integer> newMockValues = loop.map(new SSPIncrement());
		DataSet<Integer> finalMockValues = loop.closeWith(newMockValues);

		finalMockValues.print();

		env.execute();
	}

	public static final class SSPIncrement extends RichMapFunctionWithParameterServer<Integer, Integer> {
		private static final Double DEFAULT_CONVERGENCE_VALUE = 0.0;

		@Override
		public Integer map(Integer value) throws Exception {
			int currentClock = getIterationRuntimeContext().getSuperstepNumber();
			updateParameter(
					Integer.toString(getRuntimeContext().getIndexOfThisSubtask()),
					new ParameterElementImpl(currentClock, value),
					new ParameterElementImpl(currentClock, DEFAULT_CONVERGENCE_VALUE));
			return value++;
		}
	}

}
