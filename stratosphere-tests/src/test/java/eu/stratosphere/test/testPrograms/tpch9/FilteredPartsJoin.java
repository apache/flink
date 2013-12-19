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

package eu.stratosphere.test.testPrograms.tpch9;


import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.test.testPrograms.util.Tuple;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;


public class FilteredPartsJoin extends JoinFunction {
	
	private final IntPair partAndSupplierKey = new IntPair();
	private final StringValue supplyCostStr = new StringValue();
	private final Tuple ordersValue = new Tuple();
	
	/**
	 * Join together parts and orderedParts by matching partkey and suppkey.
	 * 
	 * Output Schema:
	 *  Key: suppkey
	 *  Value: (amount, year)
	 *
	 */
	@Override
	public void match(Record value1, Record value2, Collector<Record> out)
			throws Exception {
		IntPair partAndSupplierKey = value1.getField(0, this.partAndSupplierKey);
		StringValue supplyCostStr = value1.getField(1, this.supplyCostStr);
		Tuple ordersValue = value2.getField(1, this.ordersValue);
		
		IntValue year = new IntValue(Integer.parseInt(ordersValue.getStringValueAt(0)));
		float quantity = Float.parseFloat(ordersValue.getStringValueAt(1));
		float price = Float.parseFloat(ordersValue.getStringValueAt(2));
		float supplyCost = Float.parseFloat(supplyCostStr.toString());
		float amount = price - supplyCost * quantity;
		
		/* Push (supplierKey, (amount, year)): */
		value1.setField(0, partAndSupplierKey.getSecond());
		value1.setField(1, new StringIntPair(new StringValue("" + amount), year));
		out.collect(value1);
	}

}
