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

package eu.stratosphere.test.recordJobs.relational.query9Util;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.test.recordJobs.util.Tuple;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class LineItemMap extends MapFunction {
	
	/**
	 * Filter "lineitem".
	 * 
	 * Output Schema:
	 *  Key: orderkey
	 *  Value: (partkey, suppkey, quantity, price)
	 *
	 */
	@Override
	public void map(Record record, Collector<Record> out) throws Exception
	{
		Tuple inputTuple = record.getField(1, Tuple.class);
		
		/* Extract the year from the date element of the order relation: */
		
		/* pice = extendedprice * (1 - discount): */
		float price = Float.parseFloat(inputTuple.getStringValueAt(5)) * (1 - Float.parseFloat(inputTuple.getStringValueAt(6)));
		/* Project (orderkey | partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, ...) to (partkey, suppkey, quantity): */
		inputTuple.project((0 << 0) | (1 << 1) | (1 << 2) | (0 << 3) | (1 << 4));
		inputTuple.addAttribute("" + price);
		record.setField(1, inputTuple);
		out.collect(record);
	}

}
