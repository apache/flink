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

package org.apache.flink.test.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.List;

/**
 * Integration tests for {@link DataSet#project}.
 */
public class ProjectITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		/*
		 * Projection with tuple fields indexes
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple3<String, Long, Integer>> projDs = ds.
				project(3, 4, 2);
		List<Tuple3<String, Long, Integer>> result = projDs.collect();

		String expectedResult = "Hallo,1,0\n" +
				"Hallo Welt,2,1\n" +
				"Hallo Welt wie,1,2\n" +
				"Hallo Welt wie gehts?,2,3\n" +
				"ABC,2,4\n" +
				"BCD,3,5\n" +
				"CDE,2,6\n" +
				"DEF,1,7\n" +
				"EFG,1,8\n" +
				"FGH,2,9\n" +
				"GHI,1,10\n" +
				"HIJ,3,11\n" +
				"IJK,3,12\n" +
				"JKL,2,13\n" +
				"KLM,2,14\n";

		compareResultAsTuples(result, expectedResult);
	}

}
