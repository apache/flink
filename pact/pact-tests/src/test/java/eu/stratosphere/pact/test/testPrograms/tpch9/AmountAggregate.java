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

package eu.stratosphere.pact.test.testPrograms.tpch9;


import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.*;

public class AmountAggregate extends ReduceStub {
	
	private PactRecord record = new PactRecord();
	private PactString value = new PactString();
	
	/**
	 * Aggregate "amount":
	 * 
	 * sum(amount)
	 * GROUP BY nation, year
	 * 
	 * Output Schema:
	 *  Key: (nation, year)
	 *  Value: amount
	 *
	 */
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector out)
			throws Exception {

		float amount = 0;

		while (records.hasNext()) {
			record = records.next();
			record.getField(1, value);
			amount += Float.parseFloat(value.toString());
		}

		if (value != null) {
			value.setValue("" + amount);
			record.setField(1, value);
			out.collect(record);
		}
	}
	
	
	/**
	 * Creates partial sums of "amount" for each data batch:
	 */
	@Override
	public void combine(Iterator<PactRecord> records, Collector out)
			throws Exception {
		reduce(records, out);
	}

}
