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

package org.apache.flink.api.io.avro.example;

import java.io.IOException;
import java.util.Random;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class AvroTypeExample {
	
	
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<User> users = env.createInput(new UserGeneratingInputFormat());

		users
			.map(new NumberExtractingMapper())
			.groupBy(1)
			.reduceGroup(new ConcatenatingReducer())
			.print();
	}
	
	
	public static final class NumberExtractingMapper implements MapFunction<User, Tuple2<User, Integer>> {
		
		@Override
		public Tuple2<User, Integer> map(User user) {
			return new Tuple2<User, Integer>(user, user.getFavoriteNumber());
		}
	}
	
	
	public static final class ConcatenatingReducer implements GroupReduceFunction<Tuple2<User, Integer>, Tuple2<Integer, String>> {

		@Override
		public void reduce(Iterable<Tuple2<User, Integer>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
			int number = 0;
			StringBuilder colors = new StringBuilder();
			
			for (Tuple2<User, Integer> u : values) {
				number = u.f1;
				colors.append(u.f0.getFavoriteColor()).append(" - ");
			}
			
			colors.setLength(colors.length() - 3);
			out.collect(new Tuple2<Integer, String>(number, colors.toString()));
		}
	}
	
	
	public static final class UserGeneratingInputFormat extends GenericInputFormat<User> {

		private static final long serialVersionUID = 1L;
		
		private static final int NUM = 100;
		
		private final Random rnd = new Random(32498562304986L);
		
		private static final String[] NAMES = { "Peter", "Bob", "Liddy", "Alexander", "Stan" };
		
		private static final String[] COLORS = { "mauve", "crimson", "copper", "sky", "grass" };
		
		private int count;
		

		@Override
		public boolean reachedEnd() throws IOException {
			return count >= NUM;
		}

		@Override
		public User nextRecord(User reuse) throws IOException {
			count++;
			
			User u = new User();
			u.setName(NAMES[rnd.nextInt(NAMES.length)]);
			u.setFavoriteColor(COLORS[rnd.nextInt(COLORS.length)]);
			u.setFavoriteNumber(rnd.nextInt(87));
			return u;
		}
	}
}
