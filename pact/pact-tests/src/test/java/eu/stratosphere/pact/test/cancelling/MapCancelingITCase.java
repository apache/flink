/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.test.cancelling;

import org.junit.Test;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.test.testPrograms.util.DiscardingOutputFormat;
import eu.stratosphere.pact.test.testPrograms.util.InfiniteIntegerInputFormat;

public class MapCancelingITCase extends CancellingTestBase
{
	@Test
	public void testMapCancelling() throws Exception
	{
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		InfiniteIntegerInputFormat.class, "Source");
		MapContract mapper = MapContract.builder(IdentityMapper.class)
			.input(source)
			.name("Identity Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(DiscardingOutputFormat.class, mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 5 * 1000, 10 * 1000);
	}
	
	@Test
	public void testSlowMapCancelling() throws Exception
	{
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		InfiniteIntegerInputFormat.class, "Source");
		MapContract mapper = MapContract.builder(DelayingIdentityMapper.class)
			.input(source)
			.name("Delay Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(DiscardingOutputFormat.class, mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 5 * 1000, 10 * 1000);
	}
	
	@Test
	public void testMapWithLongCancellingResponse() throws Exception
	{
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		InfiniteIntegerInputFormat.class, "Source");
		MapContract mapper = MapContract.builder(LongCancelTimeIdentityMapper.class)
			.input(source)
			.name("Long Cancelling Time Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(DiscardingOutputFormat.class, mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 10 * 1000, 10 * 1000);
	}
	
	@Test
	public void testMapPriorToFirstRecordReading() throws Exception
	{
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		InfiniteIntegerInputFormat.class, "Source");
		MapContract mapper = MapContract.builder(StuckInOpenIdentityMapper.class)
			.input(source)
			.name("Stuck-In-Open Mapper")
			.build();
		GenericDataSink sink = new GenericDataSink(DiscardingOutputFormat.class, mapper, "Sink");
		
		
		Plan p = new Plan(sink);
		p.setDefaultParallelism(4);
		
		runAndCancelJob(p, 10 * 1000, 10 * 1000);
	}

	// --------------------------------------------------------------------------------------------
	
	public static final class IdentityMapper extends MapStub
	{
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception
		{
			out.collect(record);
		}
	}
	
	public static final class DelayingIdentityMapper extends MapStub
	{
		private static final int WAIT_TIME_PER_RECORD = 10 * 1000; // 10 sec.
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception
		{
			Thread.sleep(WAIT_TIME_PER_RECORD);
			out.collect(record);
		}
	}
	
	public static final class LongCancelTimeIdentityMapper extends MapStub
	{
		private static final int WAIT_TIME_PER_RECORD = 5 * 1000; // 5 sec.
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception
		{
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
	
	public static final class StuckInOpenIdentityMapper extends MapStub
	{
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.generic.AbstractStub#open(eu.stratosphere.nephele.configuration.Configuration)
		 */
		@Override
		public void open(Configuration parameters) throws Exception {
			synchronized (this) {
				wait();
			}
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			out.collect(record);
		}
	}
}
