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


package org.apache.flink.test.recordJobs.relational.query1Util;

import java.util.Iterator;

import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class GroupByReturnFlag extends ReduceFunction {

	private static final long serialVersionUID = 1L;

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
