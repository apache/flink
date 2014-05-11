/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.drivers;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

public class DriverTestData {

	public static List<Tuple2<String, Integer>> createReduceImmutableData() {
		List<Tuple2<String, Integer>> data = new ArrayList<Tuple2<String,Integer>>();
		
		data.add(new Tuple2<String, Integer>("a", 1));
		data.add(new Tuple2<String, Integer>("b", 2));
		data.add(new Tuple2<String, Integer>("c", 3));
		data.add(new Tuple2<String, Integer>("d", 4));
		data.add(new Tuple2<String, Integer>("d", 5));
		data.add(new Tuple2<String, Integer>("e", 6));
		data.add(new Tuple2<String, Integer>("e", 7));
		data.add(new Tuple2<String, Integer>("e", 8));
		data.add(new Tuple2<String, Integer>("f", 9));
		data.add(new Tuple2<String, Integer>("f", 10));
		data.add(new Tuple2<String, Integer>("f", 11));
		data.add(new Tuple2<String, Integer>("f", 12));
		
		return data;
	}
	
	public static List<Tuple2<String, Integer>> createReduceImmutableDataGroupedResult() {
		List<Tuple2<String, Integer>> data = new ArrayList<Tuple2<String,Integer>>();
		
		data.add(new Tuple2<String, Integer>("a", 1));
		data.add(new Tuple2<String, Integer>("b", 2));
		data.add(new Tuple2<String, Integer>("c", 3));
		data.add(new Tuple2<String, Integer>("dd", 9));
		data.add(new Tuple2<String, Integer>("eee", 21));
		data.add(new Tuple2<String, Integer>("ffff", 42));
		
		return data;
	}
	
	public static List<Tuple2<StringValue, IntValue>> createReduceMutableData() {
		List<Tuple2<StringValue, IntValue>> data = new ArrayList<Tuple2<StringValue, IntValue>>();
		
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("a"), new IntValue(1)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("b"), new IntValue(2)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("c"), new IntValue(3)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("d"), new IntValue(4)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("d"), new IntValue(5)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("e"), new IntValue(6)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("e"), new IntValue(7)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("e"), new IntValue(8)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("f"), new IntValue(9)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("f"), new IntValue(10)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("f"), new IntValue(11)));
		data.add(new Tuple2<StringValue, IntValue>(new StringValue("f"), new IntValue(12)));
		
		return data;
	}
	
	// --------------------------------------------------------------------------------------------
	
	private DriverTestData() {}
}
