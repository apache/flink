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
import java.util.Collection;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.operators.BulkIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.recordJobs.kmeans.udfs.PointInFormat;
import org.apache.flink.test.recordJobs.kmeans.udfs.PointOutFormat;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class IterationWithChainingITCase extends RecordAPITestBase {

	private static final String DATA_POINTS = "0|50.90|16.20|72.08|\n" + "1|73.65|61.76|62.89|\n" + "2|61.73|49.95|92.74|\n";

	private String dataPath;
	private String resultPath;

	public IterationWithChainingITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(DOP);
	}

	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("data_points.txt", DATA_POINTS);
		resultPath = getTempFilePath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(DATA_POINTS, resultPath);
	}

	@Override
	protected Plan getTestJob() {
		Plan plan = getTestPlan(config.getInteger("ChainedMapperITCase#NoSubtasks", 1), dataPath, resultPath);
		return plan;
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("ChainedMapperITCase#NoSubtasks", DOP);
		return toParameterList(config1);
	}

	public static final class IdentityMapper extends MapFunction implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record rec, Collector<Record> out) {
			out.collect(rec);
		}
	}

	public static final class DummyReducer extends ReduceFunction implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<Record> it, Collector<Record> out) {
			while (it.hasNext()) {
				out.collect(it.next());
			}
		}
	}

	static Plan getTestPlan(int numSubTasks, String input, String output) {

		FileDataSource initialInput = new FileDataSource(new PointInFormat(), input, "Input");
		initialInput.setDegreeOfParallelism(1);

		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(initialInput);
		iteration.setMaximumNumberOfIterations(2);

		ReduceOperator dummyReduce = ReduceOperator.builder(new DummyReducer(), IntValue.class, 0).input(iteration.getPartialSolution())
				.name("Reduce something").build();

		MapOperator dummyMap = MapOperator.builder(new IdentityMapper()).input(dummyReduce).build();
		iteration.setNextPartialSolution(dummyMap);

		FileDataSink finalResult = new FileDataSink(new PointOutFormat(), output, iteration, "Output");

		Plan plan = new Plan(finalResult, "Iteration with chained map test");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
}
