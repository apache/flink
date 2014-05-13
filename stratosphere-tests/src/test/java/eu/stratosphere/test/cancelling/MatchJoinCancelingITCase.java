/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.test.cancelling;

//import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.record.operators.GenericDataSink;
import eu.stratosphere.api.java.record.operators.GenericDataSource;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.recordJobs.util.DiscardingOutputFormat;
import eu.stratosphere.test.recordJobs.util.InfiniteIntegerInputFormat;
import eu.stratosphere.test.recordJobs.util.InfiniteIntegerInputFormatWithDelay;
import eu.stratosphere.test.recordJobs.util.UniformIntInput;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class MatchJoinCancelingITCase extends CancellingTestBase {
	
	// --------------- Test Sort Matches that are canceled while still reading / sorting -----------------
//	@Test
	public void testCancelSortMatchWhileReadingSlowInputs() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormatWithDelay> source1 =
			new GenericDataSource<InfiniteIntegerInputFormatWithDelay>(new InfiniteIntegerInputFormatWithDelay(), "Source 1");

		GenericDataSource<InfiniteIntegerInputFormatWithDelay> source2 =
			new GenericDataSource<InfiniteIntegerInputFormatWithDelay>(new InfiniteIntegerInputFormatWithDelay(), "Source 2");
		
		JoinOperator matcher = JoinOperator.builder(SimpleMatcher.class, IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Sort Join")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 3000, 10*1000);
	}

//	@Test
	public void testCancelSortMatchWhileReadingFastInputs() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormat> source1 =
			new GenericDataSource<InfiniteIntegerInputFormat>(new InfiniteIntegerInputFormat(), "Source 1");

		GenericDataSource<InfiniteIntegerInputFormat> source2 =
			new GenericDataSource<InfiniteIntegerInputFormat>(new InfiniteIntegerInputFormat(), "Source 2");
		
		JoinOperator matcher = JoinOperator.builder(SimpleMatcher.class, IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Sort Join")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 5000, 10*1000);
	}
	
//	@Test
	public void testCancelSortMatchPriorToFirstRecordReading() throws Exception {
		GenericDataSource<InfiniteIntegerInputFormat> source1 =
			new GenericDataSource<InfiniteIntegerInputFormat>(new InfiniteIntegerInputFormat(), "Source 1");

		GenericDataSource<InfiniteIntegerInputFormat> source2 =
			new GenericDataSource<InfiniteIntegerInputFormat>(new InfiniteIntegerInputFormat(), "Source 2");
		
		JoinOperator matcher = JoinOperator.builder(StuckInOpenMatcher.class, IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Stuc-In-Open Match")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 5000);
		
		runAndCancelJob(p, 10 * 1000, 10 * 1000);
	}
	
//	@Test
	public void testCancelSortMatchWhileDoingHeavySorting() throws Exception {
		GenericDataSource<UniformIntInput> source1 =
			new GenericDataSource<UniformIntInput>(new UniformIntInput(), "Source 1");
		source1.setParameter(UniformIntInput.NUM_KEYS_KEY, 50000);
		source1.setParameter(UniformIntInput.NUM_VALUES_KEY, 100);

		GenericDataSource<UniformIntInput> source2 =
			new GenericDataSource<UniformIntInput>(new UniformIntInput(), "Source 2");
		source2.setParameter(UniformIntInput.NUM_KEYS_KEY, 50000);
		source2.setParameter(UniformIntInput.NUM_VALUES_KEY, 100);
		
		JoinOperator matcher = JoinOperator.builder(SimpleMatcher.class, IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Long Cancelling Sort Join")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 30 * 1000, 30 * 1000);
	}
	
	
	// --------------- Test Sort Matches that are canceled while in the Matching Phase -----------------
	
//	@Test
	public void testCancelSortMatchWhileJoining() throws Exception {
		GenericDataSource<UniformIntInput> source1 =
			new GenericDataSource<UniformIntInput>(new UniformIntInput(), "Source 1");
		source1.setParameter(UniformIntInput.NUM_KEYS_KEY, 500);
		source1.setParameter(UniformIntInput.NUM_VALUES_KEY, 3);

		GenericDataSource<UniformIntInput> source2 =
			new GenericDataSource<UniformIntInput>(new UniformIntInput(), "Source 2");
		source2.setParameter(UniformIntInput.NUM_KEYS_KEY, 500);
		source2.setParameter(UniformIntInput.NUM_VALUES_KEY, 3);
		
		JoinOperator matcher = JoinOperator.builder(DelayingMatcher.class, IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Long Cancelling Sort Join")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 10 * 1000, 20 * 1000);
	}
	
//	@Test
	public void testCancelSortMatchWithLongCancellingResponse() throws Exception {
		
		GenericDataSource<UniformIntInput> source1 =
			new GenericDataSource<UniformIntInput>(new UniformIntInput(), "Source 1");
		source1.setParameter(UniformIntInput.NUM_KEYS_KEY, 500);
		source1.setParameter(UniformIntInput.NUM_VALUES_KEY, 3);

		GenericDataSource<UniformIntInput> source2 =
			new GenericDataSource<UniformIntInput>(new UniformIntInput(), "Source 2");
		source2.setParameter(UniformIntInput.NUM_KEYS_KEY, 500);
		source2.setParameter(UniformIntInput.NUM_VALUES_KEY, 3);
		
		JoinOperator matcher = JoinOperator.builder(LongCancelTimeMatcher.class, IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Long Cancelling Sort Join")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 10 * 1000, 10 * 1000);
	}

	// -------------------------------------- Test System corner cases ---------------------------------
	
//	@Test
	public void testCancelSortMatchWithHighDOP() throws Exception {
		
		GenericDataSource<InfiniteIntegerInputFormat> source1 =
			new GenericDataSource<InfiniteIntegerInputFormat>(new InfiniteIntegerInputFormat(), "Source 1");

		GenericDataSource<InfiniteIntegerInputFormat> source2 =
			new GenericDataSource<InfiniteIntegerInputFormat>(new InfiniteIntegerInputFormat(), "Source 2");
		
		JoinOperator matcher = JoinOperator.builder(new SimpleMatcher(), IntValue.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Sort Join")
			.build();
		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), matcher, "Sink");
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(64);
		
		runAndCancelJob(p, 3000, 20*1000);
	}

	// --------------------------------------------------------------------------------------------
	
	public static final class SimpleMatcher extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
			value1.setField(1, value2.getField(0, IntValue.class));
			out.collect(value1);
		}
	}
	
	public static final class DelayingMatcher extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		private static final int WAIT_TIME_PER_RECORD = 10 * 1000; // 10 sec.

		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
			Thread.sleep(WAIT_TIME_PER_RECORD);
			value1.setField(1, value2.getField(0, IntValue.class));
			out.collect(value1);
		}
	}
	
	public static final class LongCancelTimeMatcher extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		private static final int WAIT_TIME_PER_RECORD = 5 * 1000; // 5 sec.
		
		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
			value1.setField(1, value2.getField(0, IntValue.class));
			
			final long start = System.currentTimeMillis();
			long remaining = WAIT_TIME_PER_RECORD;
			do {
				try {
					Thread.sleep(remaining);
				} catch (InterruptedException iex) {}
			} while ((remaining = WAIT_TIME_PER_RECORD - System.currentTimeMillis() + start) > 0);
			
			out.collect(value1);
		}
	}
	
	public static final class StuckInOpenMatcher extends JoinFunction {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			synchronized (this) {
				wait();
			}
		}

		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
			value1.setField(1, value2.getField(0, IntValue.class));
			out.collect(value1);
		}
	}
}
