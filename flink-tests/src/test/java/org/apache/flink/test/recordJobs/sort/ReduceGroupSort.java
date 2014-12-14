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

package org.apache.flink.test.recordJobs.sort;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * This job shows how to define ordered input for a Reduce contract.
 * The inputs for CoGroups can be (individually) ordered as well.  
 */
@SuppressWarnings("deprecation")
public class ReduceGroupSort implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Increments the first field of the first record of the reduce group by 100 and emits it.
	 * Then all remaining records of the group are emitted.	 *
	 */
	@ConstantFieldsExcept(0)
	public static class IdentityReducer extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {
			
			Record next = records.next();
			
			// Increments the first field of the first record of the reduce group by 100 and emit it
			IntValue incrVal = next.getField(0, IntValue.class);
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
		CsvInputFormat format = new CsvInputFormat(' ', IntValue.class, IntValue.class);
		FileDataSource input = new FileDataSource(format, dataInput, "Input");
		
		// create the reduce contract and sets the key to the first field
		ReduceOperator sorter = ReduceOperator.builder(new IdentityReducer(), IntValue.class, 0)
			.input(input)
			.name("Reducer")
			.build();
		// sets the group sorting to the second field
		sorter.setGroupOrder(new Ordering(1, IntValue.class, Order.ASCENDING));

		// create and configure the output format
		FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, sorter, "Sorted Output");
		CsvOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(IntValue.class, 0)
			.field(IntValue.class, 1);
		
		Plan plan = new Plan(out, "SecondarySort Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
}
