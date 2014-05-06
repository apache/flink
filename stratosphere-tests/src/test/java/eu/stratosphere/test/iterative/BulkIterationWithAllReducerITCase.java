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

import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class BulkIterationWithAllReducerITCase extends RecordAPITestBase {

	private static final String IN = "1 1\n2 2\n3 3\n4 4\n5 5\n6 6\n7 7\n8 8\n";
	private static final String RESULT = "8 8\n";

	private String inputPath;
	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		inputPath = createTempFile("input.txt", IN);
		resultPath = getTempDirPath("result.txt");
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		FileDataSource input = new FileDataSource(new CsvInputFormat(), inputPath);
		CsvInputFormat.configureRecordFormat(input).fieldDelimiter(' ').field(StringValue.class, 0).field(IntValue.class, 1);

		// Use bulk iteration for iterating through input
		BulkIteration bulkIteration = new BulkIteration();
		bulkIteration.setMaximumNumberOfIterations(10);

		bulkIteration.setInput(input);

		// begin of iteration step
		ReduceOperator reduce = ReduceOperator.builder(PickOneAllReduce.class)
				.setBroadcastVariable("bc", bulkIteration.getPartialSolution()).input(input).build();

		bulkIteration.setNextPartialSolution(reduce);

		FileDataSink output = new FileDataSink(new ContractITCaseOutputFormat(), resultPath);
		output.setDegreeOfParallelism(1);
		output.setInput(bulkIteration);

		Plan plan = new Plan(output);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	public static class PickOneAllReduce extends ReduceFunction {
		private static final long serialVersionUID = 1L;

		// returns the next higher key-value-pair (if available) compared to the broadcast variable
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			Collection<Record> vars = getRuntimeContext().getBroadcastVariable("bc");
			Iterator<Record> iterator = vars.iterator();

			// Prevent bug in Iteration maxIteration+1
			if (!iterator.hasNext()) {
				return;
			}
			Record bc = iterator.next();
			int x = bc.getField(1, IntValue.class).getValue();

			boolean collected = false;
			while (records.hasNext()) {
				Record r = records.next();
				int y = r.getField(1, IntValue.class).getValue();

				if (y > x) {
					out.collect(r);
					collected = true;
					break;
				}
			}

			if (!collected) {
				out.collect(bc.createCopy());
			}
			
			// THIS SHOULD NOT BE NECESSARY!!!
			while(records.hasNext()) {
				records.next();
			}
			// THIS SHOULD NOT BE NECESSARY!!!
		}
	}
}