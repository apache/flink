/**
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

package org.apache.flink.api.java.record.io.avro.example;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.io.GenericInputFormat;
import org.apache.flink.api.java.record.operators.GenericDataSink;
import org.apache.flink.api.java.record.operators.GenericDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;


public class ReflectiveAvroTypeExample {
	
	
	public static void main(String[] args) throws Exception {
		
		GenericDataSource<UserGeneratingInputFormat> source = new GenericDataSource<UserGeneratingInputFormat>(UserGeneratingInputFormat.class);
		
		MapOperator mapper = MapOperator.builder(new NumberExtractingMapper())
				.input(source).name("le mapper").build();
		
		ReduceOperator reducer = ReduceOperator.builder(new ConcatenatingReducer(), IntValue.class, 1)
				.input(mapper).name("le reducer").build();
		
		GenericDataSink sink = new GenericDataSink(PrintingOutputFormat.class, reducer);
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		LocalExecutor.execute(p);
	}
	
	
	public static final class NumberExtractingMapper extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			User u = record.getField(0, SUser.class).datum();
			record.setField(1, new IntValue(u.getFavoriteNumber()));
			out.collect(record);
		}
	}
	
	
	public static final class ConcatenatingReducer extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final Record result = new Record(2);

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			Record r = records.next();
			
			int num = r.getField(1, IntValue.class).getValue();
			String names = r.getField(0, SUser.class).datum().getFavoriteColor().toString();
			
			while (records.hasNext()) {
				r = records.next();
				names += " - " + r.getField(0, SUser.class).datum().getFavoriteColor().toString();
			}
			
			result.setField(0, new IntValue(num));
			result.setField(1,  new StringValue(names));
			out.collect(result);
		}

	}
	
	
	public static final class UserGeneratingInputFormat extends GenericInputFormat {

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
		public Record nextRecord(Record record) throws IOException {
			count++;
			
			User u = new User();
			u.setName(NAMES[rnd.nextInt(NAMES.length)]);
			u.setFavoriteColor(COLORS[rnd.nextInt(COLORS.length)]);
			u.setFavoriteNumber(rnd.nextInt(87));
			
			SUser su = new SUser();
			su.datum(u);
			
			record.setField(0, su);
			return record;
		}
	}
	
	public static final class PrintingOutputFormat implements OutputFormat<Record> {

		private static final long serialVersionUID = 1L;

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {}

		@Override
		public void writeRecord(Record record) throws IOException {
			int color = record.getField(0, IntValue.class).getValue();
			String names = record.getField(1, StringValue.class).getValue();
			
			System.out.println(color + ": " + names);
		}
		
		@Override
		public void close() throws IOException {}
	}

}
