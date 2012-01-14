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
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.util.Tuple;

public class OrderedPartsJoin extends MatchStub
{
	/**
	 * Join "orders" and "lineitem" by "orderkey".
	 * 
	 * Output Schema:
	 *  Key: (partkey, suppkey)
	 *  Value: (year, quantity, price)
	 *
	 */
	@Override
	public void match(PactRecord value1, PactRecord value2, Collector out)
			throws Exception {

		PactInteger year = value1.getField(1, PactInteger.class);
		Tuple lineItem = value2.getField(1, Tuple.class);
		
		/* (partkey, suppkey) from lineItem: */
		IntPair newKey = new IntPair(new PactInteger(Integer.parseInt(lineItem.getStringValueAt(0))), new PactInteger(Integer.parseInt(lineItem.getStringValueAt(1))));
		Tuple newValue = new Tuple();
		newValue.addAttribute(year.toString()); // year
		newValue.addAttribute(lineItem.getStringValueAt(2)); // quantity
		newValue.addAttribute(lineItem.getStringValueAt(3)); // price
		
		value1.setField(0, newKey);
		value1.setField(1, newValue);
		out.collect(value1);
		
	}

}
