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

package eu.stratosphere.pact.example.sort;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * This job shows how to define ordered input for a Reduce contract.
 * The inputs for CoGroups can be (individually) ordered as well.  
 */
public class ReduceGroupSort implements PlanAssembler, PlanAssemblerDescription {
	
	/**
	 * Increments the first field of the first record of the reduce group by 100 and emits it.
	 * Then all remaining records of the group are emitted.	 *
	 */
	@ConstantFieldsExcept(0)
	public static class IdentityReducer extends ReduceStub implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
			
			PactRecord next = records.next();
			
			// Increments the first field of the first record of the reduce group by 100 and emit it
			PactInteger incrVal = next.getField(0, PactInteger.class);
			incrVal.setValue(incrVal.getValue() + 100);
			next.setField(0, incrVal);
			out.collect(next);
			
			// emit all remaining records
			while (records.hasNext()) {
				out.collect(records.next());
			}
		}
	}


	@Override
	public Plan getPlan(String... args) {
		
		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");

		@SuppressWarnings("unchecked")
		RecordInputFormat format = new RecordInputFormat(' ', PactInteger.class, PactInteger.class);
		FileDataSource input = new FileDataSource(format, dataInput, "Input");
		
		// create the reduce contract and sets the key to the first field
		ReduceContract sorter = ReduceContract.builder(new IdentityReducer(), PactInteger.class, 0)
			.input(input)
			.name("Reducer")
			.build();
		// sets the group sorting to the second field
		sorter.setGroupOrder(new Ordering(1, PactInteger.class, Order.ASCENDING));

		// create and configure the output format
		FileDataSink out = new FileDataSink(new RecordOutputFormat(), output, sorter, "Sorted Output");
		RecordOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0)
			.field(PactInteger.class, 1);
		
		Plan plan = new Plan(out, "SecondarySort Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
}
