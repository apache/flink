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

package org.apache.flink.test.cancelling;

//import org.junit.Test;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.operators.GenericDataSink;
import org.apache.flink.api.java.record.operators.GenericDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.recordJobs.util.DiscardingOutputFormat;
import org.apache.flink.test.recordJobs.util.InfiniteIntegerInputFormat;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class MapCancelingITCase extends CancellingTestBase {
	private static final int parallelism = 4;

	public MapCancelingITCase() {
		setTaskManagerNumSlots(parallelism);
	}
	
//	@Test
	public void testMapCancelling() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		new InfiniteIntegerInputFormat(), "Source");
		MapOperator mapper = MapOperator.builder(IdentityMapper.class)
			.input(source)
			.name("Identity Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(parallelism);
		
		runAndCancelJob(p, 5 * 1000, 10 * 1000);
	}
	
//	@Test
	public void testSlowMapCancelling() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		new InfiniteIntegerInputFormat(), "Source");
		MapOperator mapper = MapOperator.builder(DelayingIdentityMapper.class)
			.input(source)
			.name("Delay Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(parallelism);
		
		runAndCancelJob(p, 5 * 1000, 10 * 1000);
	}
	
//	@Test
	public void testMapWithLongCancellingResponse() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		new InfiniteIntegerInputFormat(), "Source");
		MapOperator mapper = MapOperator.builder(LongCancelTimeIdentityMapper.class)
			.input(source)
			.name("Long Cancelling Time Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(parallelism);
		
		runAndCancelJob(p, 10 * 1000, 10 * 1000);
	}
	
//	@Test
	public void testMapPriorToFirstRecordReading() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		new InfiniteIntegerInputFormat(), "Source");
		MapOperator mapper = MapOperator.builder(StuckInOpenIdentityMapper.class)
			.input(source)
			.name("Stuck-In-Open Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(parallelism);
		
		runAndCancelJob(p, 10 * 1000, 10 * 1000);
	}

	// --------------------------------------------------------------------------------------------
	
	public static final class IdentityMapper extends MapFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}
	}
	
	public static final class DelayingIdentityMapper extends MapFunction {
		private static final long serialVersionUID = 1L;
		
		private static final int WAIT_TIME_PER_RECORD = 10 * 1000; // 10 sec.

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			Thread.sleep(WAIT_TIME_PER_RECORD);
			out.collect(record);
		}
	}
	
	public static final class LongCancelTimeIdentityMapper extends MapFunction {
		private static final long serialVersionUID = 1L;
		
		private static final int WAIT_TIME_PER_RECORD = 5 * 1000; // 5 sec.

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			final long start = System.currentTimeMillis();
			long remaining = WAIT_TIME_PER_RECORD;
			do {
				try {
					Thread.sleep(remaining);
				} catch (InterruptedException iex) {}
			} while ((remaining = WAIT_TIME_PER_RECORD - System.currentTimeMillis() + start) > 0);
			
			out.collect(record);
		}
	}
	
	public static final class StuckInOpenIdentityMapper extends MapFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			synchronized (this) {
				wait();
			}
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}
	}
}
