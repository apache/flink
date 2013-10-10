/**
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
 */

package eu.stratosphere.pact4s.tests.perf.plainJava;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class WordCount implements PlanAssembler, PlanAssemblerDescription
{
	public static class TokenizeLine extends MapStub
	{
		private final PactString line = new PactString();
		private final PactString word = new PactString();
		private final PactInteger one = new PactInteger(1);
		private final PactRecord result = new PactRecord();

		@Override
		public void map(PactRecord record, Collector<PactRecord> out)
		{
			String line = record.getField(0, this.line).getValue();

			for (String word : line.toLowerCase().split("\\W+"))
			{
				this.word.setValue(word);
				this.result.setField(0, this.word);
				this.result.setField(1, this.one);
				out.collect(this.result);
			}
		}
	}

	@ConstantFields(fields={0})
	@OutCardBounds(lowerBound=1, upperBound=1)
	@Combinable
	public static class CountWords extends ReduceStub
	{
		private final PactInteger count = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception
		{
			PactRecord next = null;
			int count = 0;

			while (records.hasNext()) {
				next = records.next();
				count += next.getField(1, this.count).getValue();
			}

			this.count.setValue(count);
			next.setField(1, this.count);
			
			out.collect(next);
		}

		@Override
		public void combine(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception
		{
			this.reduce(records, out);
		}
	}

	@Override
	public Plan getPlan(String... args)
	{
		int numSubTasks  = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");

		MapContract mapper = MapContract.builder(TokenizeLine.class)
			.input(source).name("Tokenize Lines").build();

		ReduceContract reducer = new ReduceContract.Builder(CountWords.class, PactString.class, 0)
			.input(mapper).name("Count Words").build();

		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, reducer, "Word Counts");

		RecordOutputFormat.configureRecordFormat(out).lenient(true)
			.recordDelimiter('\n').fieldDelimiter(' ')
			.field(PactString.class, 0)
			.field(PactInteger.class, 1);

		Plan plan = new Plan(out, "WordCount");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
}
