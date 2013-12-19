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


import java.util.Iterator;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class AmountAggregate extends ReduceFunction
{
	private StringValue value = new StringValue();
	
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
	public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception
	{
		Record record = null;
		float amount = 0;

		while (records.hasNext()) {
			record = records.next();
			StringValue value = record.getField(1, StringValue.class);
			amount += Float.parseFloat(value.toString());
		}

		value.setValue(String.valueOf(amount));
		record.setField(1, value);
		out.collect(record);
	}
	
	
	/**
	 * Creates partial sums of "amount" for each data batch:
	 */
	@Override
	public void combine(Iterator<Record> records, Collector<Record> out) throws Exception
	{
		reduce(records, out);
	}
}
