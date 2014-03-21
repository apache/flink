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
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;

/**
 * #######################################################################################################
 * 
 * 			BE AWARE THAT OTHER TESTS DEPEND ON THIS TEST DATA. 
 * 			IF YOU MODIFY THE DATA MAKE SURE YOU CHECK THAT ALL TESTS ARE STILL WORKING!
 * 
 * #######################################################################################################
 */
public class CollectionDataSets {

	public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet(ExecutionEnvironment env) {
		
		List<Tuple3<Integer, Long, String>> data = new ArrayList<Tuple3<Integer, Long, String>>();
		data.add(new Tuple3<Integer, Long, String>(1,1l,"Hi"));
		data.add(new Tuple3<Integer, Long, String>(2,2l,"Hello"));
		data.add(new Tuple3<Integer, Long, String>(3,2l,"Hello world"));
		data.add(new Tuple3<Integer, Long, String>(4,3l,"Hello world, how are you?"));
		data.add(new Tuple3<Integer, Long, String>(5,3l,"I am fine."));
		data.add(new Tuple3<Integer, Long, String>(6,3l,"Luke Skywalker"));
		data.add(new Tuple3<Integer, Long, String>(7,4l,"Comment#1"));
		data.add(new Tuple3<Integer, Long, String>(8,4l,"Comment#2"));
		data.add(new Tuple3<Integer, Long, String>(9,4l,"Comment#3"));
		data.add(new Tuple3<Integer, Long, String>(10,4l,"Comment#4"));
		data.add(new Tuple3<Integer, Long, String>(11,5l,"Comment#5"));
		data.add(new Tuple3<Integer, Long, String>(12,5l,"Comment#6"));
		data.add(new Tuple3<Integer, Long, String>(13,5l,"Comment#7"));
		data.add(new Tuple3<Integer, Long, String>(14,5l,"Comment#8"));
		data.add(new Tuple3<Integer, Long, String>(15,5l,"Comment#9"));
		data.add(new Tuple3<Integer, Long, String>(16,6l,"Comment#10"));
		data.add(new Tuple3<Integer, Long, String>(17,6l,"Comment#11"));
		data.add(new Tuple3<Integer, Long, String>(18,6l,"Comment#12"));
		data.add(new Tuple3<Integer, Long, String>(19,6l,"Comment#13"));
		data.add(new Tuple3<Integer, Long, String>(20,6l,"Comment#14"));
		data.add(new Tuple3<Integer, Long, String>(21,6l,"Comment#15"));
		
		Collections.shuffle(data);
		
		TupleTypeInfo<Tuple3<Integer, Long, String>> type = new 
				TupleTypeInfo<Tuple3<Integer, Long, String>>(
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO
				);
		
		return env.fromCollection(data, type);
	}
	
	public static DataSet<Tuple5<Integer, Long, Integer, String, Long>> get5TupleDataSet(ExecutionEnvironment env) {
		
		List<Tuple5<Integer, Long, Integer, String, Long>> data = new ArrayList<Tuple5<Integer, Long, Integer, String, Long>>();
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(1,1l,0,"Hallo",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(2,2l,1,"Hallo Welt",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(2,3l,2,"Hallo Welt wie",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(3,4l,3,"Hallo Welt wie gehts?",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(3,5l,4,"ABC",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(3,6l,5,"BCD",3l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,7l,6,"CDE",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,8l,7,"DEF",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,9l,8,"EFG",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(4,10l,9,"FGH",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,11l,10,"GHI",1l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,12l,11,"HIJ",3l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,13l,12,"IJK",3l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,14l,13,"JKL",2l));
		data.add(new Tuple5<Integer, Long,  Integer, String, Long>(5,15l,14,"KLM",2l));
		
		Collections.shuffle(data);
		
		TupleTypeInfo<Tuple5<Integer, Long,  Integer, String, Long>> type = new 
				TupleTypeInfo<Tuple5<Integer, Long,  Integer, String, Long>>(
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.LONG_TYPE_INFO
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
		
		Collections.shuffle(data);
		
		return env.fromCollection(data);
	}
	
	public static DataSet<Integer> getIntegerDataSet(ExecutionEnvironment env) {
		
		List<Integer> data = new ArrayList<Integer>();
		data.add(1);
		data.add(2);
		data.add(2);
		data.add(3);
		data.add(3);
		data.add(3);
		data.add(4);
		data.add(4);
		data.add(4);
		data.add(4);
		data.add(5);
		data.add(5);
		data.add(5);
		data.add(5);
		data.add(5);
		
		Collections.shuffle(data);
		
		return env.fromCollection(data);
	}
	
	public static DataSet<CustomType> getCustomTypeDataSet(ExecutionEnvironment env) {
		
		List<CustomType> data = new ArrayList<CustomType>();
		data.add(new CustomType(1,0l,"Hi"));
		data.add(new CustomType(2,1l,"Hello"));
		data.add(new CustomType(2,2l,"Hello world"));
		data.add(new CustomType(3,3l,"Hello world, how are you?"));
		data.add(new CustomType(3,4l,"I am fine."));
		data.add(new CustomType(3,5l,"Luke Skywalker"));
		data.add(new CustomType(4,6l,"Comment#1"));
		data.add(new CustomType(4,7l,"Comment#2"));
		data.add(new CustomType(4,8l,"Comment#3"));
		data.add(new CustomType(4,9l,"Comment#4"));
		data.add(new CustomType(5,10l,"Comment#5"));
		data.add(new CustomType(5,11l,"Comment#6"));
		data.add(new CustomType(5,12l,"Comment#7"));
		data.add(new CustomType(5,13l,"Comment#8"));
		data.add(new CustomType(5,14l,"Comment#9"));
		data.add(new CustomType(6,15l,"Comment#10"));
		data.add(new CustomType(6,16l,"Comment#11"));
		data.add(new CustomType(6,17l,"Comment#12"));
		data.add(new CustomType(6,18l,"Comment#13"));
		data.add(new CustomType(6,19l,"Comment#14"));
		data.add(new CustomType(6,20l,"Comment#15"));
		
		Collections.shuffle(data);
		
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
