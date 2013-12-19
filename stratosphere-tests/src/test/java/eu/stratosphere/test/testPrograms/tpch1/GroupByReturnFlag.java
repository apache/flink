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

package eu.stratosphere.test.testPrograms.tpch1;

import java.util.Iterator;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.test.testPrograms.util.Tuple;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 */
public class GroupByReturnFlag extends ReduceFunction {

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stub.ReduceStub#reduce(eu.stratosphere.pact.common.type.Key, java.util.Iterator, eu.stratosphere.pact.common.stub.Collector)
	 */
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
		Record outRecord = new Record();
		Tuple returnTuple = new Tuple();
		
		long quantity = 0;
		double extendedPriceSum = 0.0; 
		
		boolean first = true;
		while(records.hasNext()) {
			Record rec = records.next();
			Tuple t = rec.getField(1, Tuple.class);
			
			if(first) {
				first = false;
				rec.copyTo(outRecord);
				returnTuple.addAttribute(rec.getField(0, StringValue.class).toString());
			}
			
			long tupleQuantity = Long.parseLong(t.getStringValueAt(4));
			quantity += tupleQuantity;
			
			double extendedPricePerTuple = Double.parseDouble(t.getStringValueAt(5));
			extendedPriceSum += extendedPricePerTuple;
		}
		
		LongValue pactQuantity = new LongValue(quantity);
		returnTuple.addAttribute("" + pactQuantity);
		returnTuple.addAttribute("" + extendedPriceSum);
		
		outRecord.setField(1, returnTuple);
		out.collect(outRecord);
	}

}
