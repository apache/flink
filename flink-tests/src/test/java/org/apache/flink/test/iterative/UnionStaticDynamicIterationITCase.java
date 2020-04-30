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

package org.apache.flink.test.iterative;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * Test union between static and dynamic path in an iteration.
 */
public class UnionStaticDynamicIterationITCase  extends JavaProgramTestBase {

	private final ArrayList<Long> result = new ArrayList<Long>();

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> inputStatic = env.generateSequence(1, 4);
		DataSet<Long> inputIteration = env.generateSequence(1, 4);

		IterativeDataSet<Long> iteration = inputIteration.iterate(3);

		DataSet<Long> result = iteration.closeWith(inputStatic.union(inputStatic).union(iteration.union(iteration)));

		result.output(new LocalCollectionOutputFormat<Long>(this.result));

		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		assertEquals(88, result.size());
	}
}
