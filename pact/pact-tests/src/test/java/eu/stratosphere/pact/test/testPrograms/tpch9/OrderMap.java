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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class OrderMap extends MapStub {
	
	private final Tuple inputTuple = new Tuple();
	
	/**
	 * Project "orders"
	 * 
	 * Output Schema:
	 *  Key: orderkey
	 *  Value: year (from date)
	 *
	 */
	@Override
	public void map(PactRecord record, Collector out) throws Exception {
		record.getField(1, inputTuple);
		
		int year = Integer.parseInt(inputTuple.getStringValueAt(4).substring(0, 4));
		record.setField(1, new PactInteger(year));
		out.collect(record);
	}

}
