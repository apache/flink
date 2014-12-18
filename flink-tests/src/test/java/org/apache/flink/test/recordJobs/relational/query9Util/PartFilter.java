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

import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings({ "deprecation", "serial" })
public class PartFilter extends MapFunction {

	private final Tuple inputTuple = new Tuple();
	
	private static String COLOR = "green";
	
	/**
	 * Filter and project "part".
	 * The parts are filtered by "name LIKE %green%".
	 * 
	 * Output Schema:
	 *  Key: partkey
	 *  Value: (empty)
	 *
	 */
	@Override
	public void map(Record record, Collector<Record> out) throws Exception
	{
		Tuple inputTuple = record.getField(1, this.inputTuple);
		if (inputTuple.getStringValueAt(1).indexOf(COLOR) != -1) {
			record.setField(1, NullValue.getInstance());
			out.collect(record);
		}
	}
}
