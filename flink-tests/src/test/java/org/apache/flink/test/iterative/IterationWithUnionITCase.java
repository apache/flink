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

import java.io.Serializable;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.recordJobs.kmeans.udfs.PointInFormat;
import org.apache.flink.test.recordJobs.kmeans.udfs.PointOutFormat;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

public class IterationWithUnionITCase extends JavaProgramTestBase {

	private static final String DATAPOINTS = "0|50.90|16.20|72.08|\n" + "1|73.65|61.76|62.89|\n" + "2|61.73|49.95|92.74|\n";

	protected String dataPath;
	protected String resultPath;


	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("datapoints.txt", DATAPOINTS);
		resultPath = getTempDirPath("union_iter_result");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(DATAPOINTS + DATAPOINTS + DATAPOINTS + DATAPOINTS, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Record> initialInput = env.readFile(new PointInFormat(), this.dataPath).setParallelism(1);
		
		IterativeDataSet<Record> iteration = initialInput.iterate(2);
		
		DataSet<Record> result = iteration.union(iteration).map(new IdentityMapper());
		
		iteration.closeWith(result).write(new PointOutFormat(), this.resultPath);
		
		env.execute();
	}
	
	static final class IdentityMapper implements MapFunction<Record, Record>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public Record map(Record rec) {
			return rec;
		}
	}

	static class DummyReducer implements GroupReduceFunction<Record, Record>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Record> it, Collector<Record> out) {
			for (Record r : it) {
				out.collect(r);
			}
		}
	}
}
