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

package eu.stratosphere.test.testPrograms.mergeOnlyJoin;

import java.util.Iterator;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.record.io.CsvInputFormat;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class MergeOnlyJoin implements Program, ProgramDescription {

	@ConstantFieldsFirstExcept(2)
	public static class JoinInputs extends JoinFunction {
		@Override
		public void match(Record input1, Record input2, Collector<Record> out) {
			input1.setField(2, input2.getField(1, IntValue.class));
			out.collect(input1);
		}
	}

	@ConstantFieldsExcept({})
	public static class DummyReduce extends ReduceFunction {
		
		@Override
		public void reduce(Iterator<Record> values, Collector<Record> out) {
			while (values.hasNext()) {
				out.collect(values.next());
			}
		}
	}


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
		CsvInputFormat format1 = new CsvInputFormat('|', IntValue.class, IntValue.class);
		FileDataSource input1 = new FileDataSource(format1, input1Path, "Input 1");
		
		ReduceOperator aggInput1 = ReduceOperator.builder(DummyReduce.class, IntValue.class, 0)
			.input(input1)
			.name("AggOrders")
			.build();

		
		// create DataSourceContract for Orders input
		@SuppressWarnings("unchecked")
		CsvInputFormat format2 = new CsvInputFormat('|', IntValue.class, IntValue.class);
		FileDataSource input2 = new FileDataSource(format2, input2Path, "Input 2");
		input2.setDegreeOfParallelism(numSubtasksInput2);

		ReduceOperator aggInput2 = ReduceOperator.builder(DummyReduce.class, IntValue.class, 0)
			.input(input2)
			.name("AggLines")
			.build();
		aggInput2.setDegreeOfParallelism(numSubtasksInput2);
		
		// create JoinOperator for joining Orders and LineItems
		JoinOperator joinLiO = JoinOperator.builder(JoinInputs.class, IntValue.class, 0, 0)
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
			.field(IntValue.class, 0)
			.field(IntValue.class, 1)
			.field(IntValue.class, 2);
		
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