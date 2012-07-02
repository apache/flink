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

package eu.stratosphere.pact.example.secondarysort;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * 
 * @author Aljoscha Krettek
 */
public class CoGroupSecondarySort implements PlanAssembler, PlanAssemblerDescription {
	/**
	 * Writes <tt>PactRecord</tt> containing two integers (key, value) to a
	 * file. The output format is: "&lt;key&gt; &lt;value&gt;\n"
	 */
	public static class SecondarySortOutFormat extends FileOutputFormat {
		private final StringBuilder buffer = new StringBuilder();

		@Override
		public void writeRecord(PactRecord record) throws IOException {
			this.buffer.setLength(0);
			this.buffer.append(record.getField(0, PactInteger.class).getValue());
			this.buffer.append(' ');
			this.buffer.append(record.getField(1, PactInteger.class).getValue());
			this.buffer.append('\n');

			byte[] bytes = this.buffer.toString().getBytes();
			this.stream.write(bytes);
		}
	}

	/**
	 * Converts a key/value line into a PactRecord
	 */
	public static class TokenizeLine extends MapStub {
		private final PactRecord outputRecord = new PactRecord();

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector) {
			// get the first field (as type PactString) from the record
			PactString str = record.getField(0, PactString.class);
			if (str.toString().equals("")) {
				return;
			}
			String[] parts = str.toString().split(" ");
			int key = Integer.parseInt(parts[0]);
			int value = Integer.parseInt(parts[1]);
			outputRecord.setField(0, new PactInteger(key));
			outputRecord.setField(1, new PactInteger(value));
			collector.collect(outputRecord);
		}
	}

	public static class IdentityReducer extends ReduceStub {
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
			PactRecord next = records.next();
			out.collect(new PactRecord(new PactInteger(100 + next.getField(0, PactInteger.class).getValue()), next
					.getField(1, PactInteger.class)));
			while (records.hasNext()) {
				out.collect(records.next());
			}
		}

	}

	public static class IdentityCoGroup extends CoGroupStub {
		@Override
		public void coGroup(Iterator<PactRecord> records, Iterator<PactRecord> records2, Collector<PactRecord> out) {
			PactRecord next = records.next();
			out.collect(new PactRecord(new PactInteger(100 + next.getField(0, PactInteger.class).getValue()), next
					.getField(1, PactInteger.class)));
			while (records.hasNext()) {
				out.collect(records.next());
			}
			next = records2.next();
			out.collect(new PactRecord(new PactInteger(100 + next.getField(0, PactInteger.class).getValue()), next
					.getField(1, PactInteger.class)));
			while (records2.hasNext()) {
				out.collect(records2.next());
			}
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String dataInput2 = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
		FileDataSource source2 = new FileDataSource(TextInputFormat.class, dataInput2, "Input Lines");
		MapContract mapper = new MapContract(TokenizeLine.class, source, "Tokenize Lines");
		MapContract mapper2 = new MapContract(TokenizeLine.class, source2, "Tokenize Lines");
		CoGroupContract coGroup = new CoGroupContract(IdentityCoGroup.class, PactInteger.class, 0, 0);
		coGroup.setSecondarySortKeyClasses(new Class[] { PactInteger.class });
		coGroup.setSecondarySortKeyColumnNumbers(0, new int[] { 1 });
		coGroup.setSecondarySortKeyColumnNumbers(1, new int[] { 1 });
		coGroup.setFirstInput(mapper);
		coGroup.setSecondInput(mapper2);
		FileDataSink out = new FileDataSink(SecondarySortOutFormat.class, output, coGroup, "Sorted entries");

		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII"); // comment
																	// out this
																	// line for
																	// UTF-8
																	// inputs
		source2.setParameter(TextInputFormat.CHARSET_NAME, "ASCII"); // comment
																		// out
																		// this
																		// line
																		// for
																		// UTF-8
																		// inputs

		Plan plan = new Plan(out, "SecondarySort Example");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input] [input2] [output]";
	}

}
