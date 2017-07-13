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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test empty (identity) bulk iteration.
 */
public class IdentityIterationITCase extends JavaProgramTestBase {

	private List<Long> result = new ArrayList<Long>();

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		IterativeDataSet<Long> iteration = env.generateSequence(1, 10).iterate(100);
		iteration.closeWith(iteration)
			.output(new LocalCollectionOutputFormat<Long>(result));

		env.execute();
	}

	@Override
	protected void postSubmit()  {
		assertEquals(10, result.size());

		long sum = 0;
		for (Long l : result) {
			sum += l;
		}
		assertEquals(55, sum);
	}
}
