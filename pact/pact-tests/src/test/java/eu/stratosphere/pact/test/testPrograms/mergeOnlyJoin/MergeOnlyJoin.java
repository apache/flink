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

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;

public class MergeOnlyJoin implements PlanAssembler, PlanAssemblerDescription {

	@ConstantFieldsFirstExcept(fields={2})
	@OutCardBounds(upperBound=1, lowerBound=1)
	public static class JoinInputs extends MatchStub
	{
		@Override
		public void match(PactRecord input1, PactRecord input2, Collector<PactRecord> out)
		{
			input1.setField(2, input2.getField(1, PactInteger.class));
			out.collect(input1);
		}
	}

	@ConstantFieldsExcept(fields={})
	public static class DummyReduce extends ReduceStub
	{
		
		@Override
		public void reduce(Iterator<PactRecord> values, Collector<PactRecord> out)
		{
			while (values.hasNext()) {
				out.collect(values.next());
			}
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(final String... args) 
	{
		// parse program parameters
		int noSubtasks       = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String input1Path    = (args.length > 1 ? args[1] : "");
		String input2Path    = (args.length > 2 ? args[2] : "");
		String output        = (args.length > 3 ? args[3] : "");
		int noSubtasksInput2 = (args.length > 4 ? Integer.parseInt(args[4]) : 1);

		// create DataSourceContract for Orders input
		FileDataSource input1 = new FileDataSource(RecordInputFormat.class, input1Path, "Input 1");
		input1.setDegreeOfParallelism(noSubtasks);
		
		input1.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		input1.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		input1.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);

		input1.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		input1.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 0);

		input1.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		input1.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 1);
		
		
		ReduceContract aggInput1 = new ReduceContract.Builder(DummyReduce.class, PactInteger.class, 0)
			.input(input1)
			.name("AggOrders")
			.build();
		aggInput1.setDegreeOfParallelism(noSubtasks);

		
		// create DataSourceContract for Orders input
		FileDataSource input2 = new FileDataSource(RecordInputFormat.class, input2Path, "Input 2");
		input2.setDegreeOfParallelism(noSubtasksInput2);
		
		input2.setParameter(RecordInputFormat.RECORD_DELIMITER, "\n");
		input2.setParameter(RecordInputFormat.FIELD_DELIMITER_PARAMETER, "|");
		input2.setParameter(RecordInputFormat.NUM_FIELDS_PARAMETER, 2);
		// order id
		input2.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+0, DecimalTextIntParser.class);
		input2.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+0, 0);
		// ship prio
		input2.getParameters().setClass(RecordInputFormat.FIELD_PARSER_PARAMETER_PREFIX+1, DecimalTextIntParser.class);
		input2.setParameter(RecordInputFormat.TEXT_POSITION_PARAMETER_PREFIX+1, 1);

		ReduceContract aggInput2 = new ReduceContract.Builder(DummyReduce.class, PactInteger.class, 0)
			.input(input2)
			.name("AggLines")
			.build();
		aggInput2.setDegreeOfParallelism(noSubtasksInput2);		
		
		// create MatchContract for joining Orders and LineItems
		MatchContract joinLiO = MatchContract.builder(JoinInputs.class, PactInteger.class, 0, 0)
			.input1(aggInput1)
			.input2(aggInput2)
			.name("JoinLiO")
			.build();
		joinLiO.setDegreeOfParallelism(noSubtasks);
		// compiler hints


		// create DataSinkContract for writing the result
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, joinLiO, "Output");
		result.setDegreeOfParallelism(noSubtasks);
		result.getParameters().setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
		result.getParameters().setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, "|");
		result.getParameters().setBoolean(RecordOutputFormat.LENIENT_PARSING, true);
		result.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 3);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactInteger.class);
		result.getParameters().setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 0);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
		result.getParameters().setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 1);
		result.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 2, PactInteger.class);
		result.getParameters().setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 2, 2);
		
		// assemble the PACT plan
		Plan plan = new Plan(result, "Merge Only Join");
		plan.setDefaultParallelism(noSubtasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubTasks], [input], [input2], [output], [noSubTasksInput2]";
	}

}