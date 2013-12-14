/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.test.testPrograms.mergeOnlyJoin;

import java.util.Iterator;

import eu.stratosphere.api.functions.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.functions.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.api.plan.PlanAssembler;
import eu.stratosphere.api.plan.PlanAssemblerDescription;
import eu.stratosphere.api.record.functions.MatchStub;
import eu.stratosphere.api.record.functions.ReduceStub;
import eu.stratosphere.api.record.io.CsvInputFormat;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

public class MergeOnlyJoin implements PlanAssembler, PlanAssemblerDescription {

	@ConstantFieldsFirstExcept(2)
	public static class JoinInputs extends MatchStub {
		@Override
		public void match(PactRecord input1, PactRecord input2, Collector<PactRecord> out) {
			input1.setField(2, input2.getField(1, PactInteger.class));
			out.collect(input1);
		}
	}

	@ConstantFieldsExcept({})
	public static class DummyReduce extends ReduceStub {
		
		@Override
		public void reduce(Iterator<PactRecord> values, Collector<PactRecord> out) {
			while (values.hasNext()) {
				out.collect(values.next());
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(final String... args) {
		// parse program parameters
		int numSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String input1Path    = (args.length > 1 ? args[1] : "");
		String input2Path    = (args.length > 2 ? args[2] : "");
		String output        = (args.length > 3 ? args[3] : "");
		int numSubtasksInput2 = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		// create DataSourceContract for Orders input
		@SuppressWarnings("unchecked")
		CsvInputFormat format1 = new CsvInputFormat('|', PactInteger.class, PactInteger.class);
		FileDataSource input1 = new FileDataSource(format1, input1Path, "Input 1");
		
		ReduceOperator aggInput1 = ReduceOperator.builder(DummyReduce.class, PactInteger.class, 0)
			.input(input1)
			.name("AggOrders")
			.build();

		
		// create DataSourceContract for Orders input
		@SuppressWarnings("unchecked")
		CsvInputFormat format2 = new CsvInputFormat('|', PactInteger.class, PactInteger.class);
		FileDataSource input2 = new FileDataSource(format2, input2Path, "Input 2");
		input2.setDegreeOfParallelism(numSubtasksInput2);

		ReduceOperator aggInput2 = ReduceOperator.builder(DummyReduce.class, PactInteger.class, 0)
			.input(input2)
			.name("AggLines")
			.build();
		aggInput2.setDegreeOfParallelism(numSubtasksInput2);
		
		// create JoinOperator for joining Orders and LineItems
		JoinOperator joinLiO = JoinOperator.builder(JoinInputs.class, PactInteger.class, 0, 0)
			.input1(aggInput1)
			.input2(aggInput2)
			.name("JoinLiO")
			.build();

		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, joinLiO, "Output");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.lenient(true)
			.field(PactInteger.class, 0)
			.field(PactInteger.class, 1)
			.field(PactInteger.class, 2);
		
		// assemble the PACT plan
		Plan plan = new Plan(result, "Merge Only Join");
		plan.setDefaultParallelism(numSubtasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubTasks], [input], [input2], [output], [numSubTasksInput2]";
	}
}