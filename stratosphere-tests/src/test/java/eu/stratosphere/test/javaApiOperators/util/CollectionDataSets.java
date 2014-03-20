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
package eu.stratosphere.test.javaApiOperators.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;

public class CollectionDataSets {

	public static DataSet<Tuple3<Integer, Long, String>> getTupleDataSet(ExecutionEnvironment env) {
		
		List<Tuple3<Integer, Long, String>> data = new ArrayList<Tuple3<Integer, Long, String>>();
		data.add(new Tuple3<Integer, Long, String>(1,10l,"Hi"));
		data.add(new Tuple3<Integer, Long, String>(-1,11l,"Hello"));
		data.add(new Tuple3<Integer, Long, String>(10,12l,"Hello world"));
		data.add(new Tuple3<Integer, Long, String>(-10,13l,"Hello world, how are you?"));
		data.add(new Tuple3<Integer, Long, String>(100,14l,"I am fine."));
		data.add(new Tuple3<Integer, Long, String>(1000,15l,"Luke Skywalker"));
		data.add(new Tuple3<Integer, Long, String>(-1000,16l,"Random comment"));
		data.add(new Tuple3<Integer, Long, String>(-100,17l,"LOL"));
		
		TupleTypeInfo<Tuple3<Integer, Long, String>> type = new 
				TupleTypeInfo<Tuple3<Integer, Long, String>>(
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO
				);
		
		return env.fromCollection(data, type);
	}
	
	public static DataSet<String> getStringDataSet(ExecutionEnvironment env) {
		
		List<String> data = new ArrayList<String>();
		data.add("Hi");
		data.add("Hello");
		data.add("Hello world");
		data.add("Hello world, how are you?");
		data.add("I am fine.");
		data.add("Luke Skywalker");
		data.add("Random comment");
		data.add("LOL");
		
		return env.fromCollection(data, BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	public static DataSet<CustomType> getCustomTypeDataSet(ExecutionEnvironment env) {
		
		List<CustomType> data = new ArrayList<CustomType>();
		data.add(new CustomType(1,10l,"Hi"));
		data.add(new CustomType(-1,11l,"Hello"));
		data.add(new CustomType(10,12l,"Hello world"));
		data.add(new CustomType(-10,13l,"Hello world, how are you?"));
		data.add(new CustomType(100,14l,"I am fine."));
		data.add(new CustomType(1000,15l,"Luke Skywalker"));
		data.add(new CustomType(-1000,16l,"Random comment"));
		data.add(new CustomType(-100,17l,"LOL"));
		
		return env.fromCollection(data);
		
	}
	
	public static class CustomType implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		public int myInt;
		public long myLong;
		public String myString;
		
		public CustomType() {};
		
		public CustomType(int i, long l, String s) {
			myInt = i;
			myLong = l;
			myString = s;
		}
		
		@Override
		public String toString() {
			return myInt+","+myLong+","+myString;
		}
		
	}
	
}
