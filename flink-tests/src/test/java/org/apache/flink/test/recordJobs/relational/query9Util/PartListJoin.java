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


import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PartListJoin extends JoinFunction {

	private final StringIntPair amountYearPair = new StringIntPair();
	private final StringValue nationName = new StringValue();
	
	/**
	 * Join "filteredParts" and "suppliers" by "suppkey".
	 * 
	 * Output Schema:
	 *  Key: (nation, year)
	 *  Value: amount
	 *
	 */
	@Override
	public void join(Record value1, Record value2, Collector<Record> out) throws Exception
	{
		StringIntPair amountYearPair = value1.getField(1, this.amountYearPair);
		StringValue nationName = value2.getField(1, this.nationName);
		
		IntValue year = amountYearPair.getSecond();
		StringValue amount = amountYearPair.getFirst();
		StringIntPair key = new StringIntPair(nationName, year);
		value1.setField(0, key);
		value1.setField(1, amount);
		out.collect(value1);
	}

}
