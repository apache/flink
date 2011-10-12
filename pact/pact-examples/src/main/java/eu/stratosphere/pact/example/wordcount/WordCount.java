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

package eu.stratosphere.pact.example.wordcount;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;



/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 * 
 * @author Larysa, Moritz Kaufmann, Stephan Ewen
 */
public class WordCount implements PlanAssembler, PlanAssemblerDescription
{
	/**
	 * Converts a input line, assuming to contain a string, into a record that has a single field,
	 * which is a {@link PactString}, containing that line.
	 */
	public static class LineInFormat extends DelimitedInputFormat
	{
		private final PactString string = new PactString();
		
		@Override
		public boolean readRecord(PactRecord record, byte[] line, int numBytes)
		{
			this.string.setValueAscii(line, 0, numBytes);
			record.setField(0, this.string);
			return true;
		}
	}

	/**
	 * Writes <tt>PactRecord</tt> containing an string (word) and an integer (count) to a file.
	 * The output format is: "&lt;word&gt; &lt;count&gt;\n"
	 */
	public static class WordCountOutFormat extends FileOutputFormat
	{
		private final StringBuilder buffer = new StringBuilder();
		
		@Override
		public void writeRecord(PactRecord record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, PactString.class).toString());
			this.buffer.append(' ');
			this.buffer.append(record.getField(1, PactInteger.class).getValue());
			this.buffer.append('\n');
			
			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	/**
	 * Converts a PactRecord containing one string in to multiple string/integer pairs.
	 * The string is tokenized by whitespaces. For each token a new record is emitted,
	 * where the Token is the first field and an Integer(1) is the second field.
	 */
	public static class TokenizeLine extends MapStub
	{
		private final PactRecord outputRecord = new PactRecord();
		private final PactString string = new PactString();
		private final PactInteger integer = new PactInteger(1);
		
		private final AsciiUtils.WhitespaceTokenizer tokenizer = 
						new AsciiUtils.WhitespaceTokenizer();
		
		@Override
		public void map(PactRecord record, Collector collector)
		{
			// get the first field (as type PactString) from the record
			PactString str = record.getField(0, PactString.class);
			
			// normalize the line
			AsciiUtils.replaceNonWordChars(str, ' ');
			AsciiUtils.toLowerCase(str);
			
			// tokenize the line
			this.tokenizer.setStringToTokenize(str);
			while (tokenizer.next(this.string))
			{
				// we emit a (word, 1) pair 
				this.outputRecord.setField(0, this.string);
				this.outputRecord.setField(1, this.integer);
				collector.collect(this.outputRecord);
			}
		}
	}

	/**
	 * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
	 * in the record. The other fields are not modified.
	 */
	@Combinable
	public static class CountWords extends ReduceStub
	{
		private final PactInteger theInteger = new PactInteger();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector out) throws Exception
		{
			PactRecord element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				element.getField(1, this.theInteger);
				// we could have equivalently used PactInteger i = record.getField(1, PactInteger.class);
				sum += this.theInteger.getValue();
			}

			this.theInteger.setValue(sum);
			element.setField(1, this.theInteger);
			out.collect(element);
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#combine(java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void combine(Iterator<PactRecord> records, Collector out) throws Exception
		{
			// the logic is the same as in the reduce function, so simply call the reduce method
			this.reduce(records, out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args)
	{
		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(LineInFormat.class, dataInput, "Input Lines");
		MapContract mapper = new MapContract(TokenizeLine.class, source, "Tokenize Lines");
		ReduceContract reducer = new ReduceContract(CountWords.class, 0, PactString.class, mapper, "Count Words");
		FileDataSink out = new FileDataSink(WordCountOutFormat.class, output, reducer, "Output");

		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input] [output]";
	}

}
