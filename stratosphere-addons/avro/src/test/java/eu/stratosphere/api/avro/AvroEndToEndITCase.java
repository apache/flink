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
package eu.stratosphere.api.avro;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.common.io.UnsplittableInput;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.GenericInputFormat;
import eu.stratosphere.api.java.record.io.avro.generated.Colors;
import eu.stratosphere.api.java.record.io.avro.generated.User;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;


public class AvroEndToEndITCase extends TestBase2 {
	
	@Override
	protected Plan getTestJob() {
		GenericDataSource<CollectionInputFormat> source = new GenericDataSource<CollectionInputFormat>(new CollectionInputFormat());
		
		MapOperator mapper = MapOperator.builder(new NameExtractor()).input(source).build();
		
		ReduceOperator reducer = ReduceOperator.builder(new IdReducer()).keyField(StringValue.class, 0).input(mapper).build();
		
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), reducer);
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		return p;
	}
	
	// --------------------------------------------------------------------------------------------
	
	// --------------------------------------------------------------------------------------------
	
	public static final class SUser extends AvroBaseValue<User> {
		
		static final long serialVersionUID = 1L;

		public SUser() {}
	
		public SUser(User u) {
			super(u);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	// --------------------------------------------------------------------------------------------
	
	public static final class NameExtractor extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			SUser su = record.getField(0, SUser.class);
			User u = su.datum();
			
			out.collect(new Record(new StringValue(u.getName()), su));
		}
		
	}
	
	public static final class IdReducer extends ReduceFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			while (records.hasNext()) {
				out.collect(records.next());
			}
		}
	}
	
	public static final class CollectionInputFormat extends GenericInputFormat implements UnsplittableInput {

		private static final long serialVersionUID = 1L;
		
		private final Random rnd = new Random(2389756789345689276L);
		
		private int num = 40;
		
		
		@Override
		public void open(GenericInputSplit split) throws IOException {}
		
		@Override
		public boolean reachedEnd() throws IOException {
			return num <= 0;
		}

		@Override
		public boolean nextRecord(Record record) throws IOException {
			num--;
			
			User u = randomUser();
			record.setField(0, new SUser(u));
			
			return true;
		}
		
		private User randomUser() {
			List<CharSequence> strings = new ArrayList<CharSequence>();
			{
				int numStrings = this.rnd.nextInt(20);
				for (int i = 0; i < numStrings; i++) {
					strings.add(randomString());
				}
			}
			
			List<Boolean> bools = new ArrayList<Boolean>();
			{
				int numBools = this.rnd.nextInt(100);
				for (int i = 0; i < numBools; i++) {
					bools.add(this.rnd.nextBoolean());
				}
			}
			
			Map<CharSequence, Long> map = new HashMap<CharSequence, Long>();
			{
				int numMapEntries = this.rnd.nextInt(10);
				for (int i = 0; i < numMapEntries; i++) {
					long val = this.rnd.nextLong();
					map.put(String.valueOf(val), val);
				}
			}
			return new User(randomString(), rnd.nextInt(10000), randomString(), rnd.nextLong(), rnd.nextDouble(), null, true, strings, bools, null,
				Colors.values()[rnd.nextInt(Colors.values().length)], map);
		}
		
		private String randomString() {
			char[] c = new char[this.rnd.nextInt(20)];
			
			for (int i = 0; i < c.length; i++) {
				c[i] = (char) this.rnd.nextInt(16000);
			}
			
			return new String(c);
		}
	}
	
	public static final class DiscardingOutputFormat implements OutputFormat<Record> {

		private static final long serialVersionUID = 1L;
				
		
		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) {}

		@Override
		public void writeRecord(Record element) {
			element.getField(1, SUser.class);
		}
		
		@Override
		public void close() {}
	}
}
