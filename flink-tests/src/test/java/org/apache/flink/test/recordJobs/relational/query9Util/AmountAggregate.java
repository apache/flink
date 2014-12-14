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

package org.apache.flink.test.recordJobs.relational.query9Util;

import java.util.Iterator;

import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

@SuppressWarnings({"serial", "deprecation"})
public class AmountAggregate extends ReduceFunction {
	
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
