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
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

@SuppressWarnings({ "deprecation", "serial" })
public class SuppliersJoin extends JoinFunction {
	
	private IntValue suppKey = new IntValue();
	private Tuple nationVal = new Tuple();
	
	/**
	 * Join "nation" and "supplier" by "nationkey".
	 * 
	 * Output Schema:
	 *  Key: suppkey
	 *  Value: "nation" (name of the nation)
	 *
	 */
	@Override
	public void join(Record value1, Record value2, Collector<Record> out)
			throws Exception {
		suppKey = value1.getField(1, suppKey);
		nationVal = value2.getField(1, nationVal);
		
		StringValue nationName = new StringValue(nationVal.getStringValueAt(1));
		
		value1.setField(0, suppKey);
		value1.setField(1, nationName);
		
		out.collect(value1);
		
	}

}
