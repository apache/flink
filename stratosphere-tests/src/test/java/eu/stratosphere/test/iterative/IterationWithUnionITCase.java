/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.operators.BulkIteration;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.functions.MapFunction;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.kmeans.udfs.PointInFormat;
import eu.stratosphere.example.record.kmeans.udfs.PointOutFormat;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@RunWith(Parameterized.class)
public class IterationWithUnionITCase extends TestBase2 {

	private static final String DATAPOINTS = "0|50.90|16.20|72.08|\n" + "1|73.65|61.76|62.89|\n" + "2|61.73|49.95|92.74|\n";

	protected String dataPath;
	protected String resultPath;

	
	public IterationWithUnionITCase(Configuration config) {
		super(config);
	}

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
	protected Plan getTestJob() {
		return getPlan(config.getInteger("IterationWithUnionITCase#NumSubtasks", 1), dataPath, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("IterationWithUnionITCase#NumSubtasks", 4);

		return toParameterList(config1);
	}
	
	private static Plan getPlan(int numSubTasks, String input, String output) {
		FileDataSource initialInput = new FileDataSource(new PointInFormat(), input, "Input");
		initialInput.setDegreeOfParallelism(1);
		
		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(initialInput);
		iteration.setMaximumNumberOfIterations(2);

//		MapOperator map1 = MapOperator.builder(new IdentityMapper()).input(iteration.getPartialSolution()).name("map1").build();
		MapOperator map2 = MapOperator.builder(new IdentityMapper()).input(iteration.getPartialSolution()).name("map").build();
		map2.addInput(iteration.getPartialSolution());
		
		iteration.setNextPartialSolution(map2);

		FileDataSink finalResult = new FileDataSink(new PointOutFormat(), output, iteration, "Output");

		Plan plan = new Plan(finalResult, "Iteration with union test");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
	
	static final class IdentityMapper extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record rec, Collector<Record> out) {
			out.collect(rec);
		}
	}

	static final class DummyReducer extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<Record> it, Collector<Record> out) {
			while (it.hasNext()) {
				out.collect(it.next());
			}
		}
	}
}
