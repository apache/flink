/***********************************************************************************************************************
*
* Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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

@RunWith(Parameterized.class)
public class SimpleJobCancelingTest extends CancellingTestBase
{
	public SimpleJobCancelingTest(Configuration config) {
		super(config);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.test.cancelling.CancellingTestBase#getTestPlan()
	 */
	@Override
	protected Plan getTestPlan() throws Exception
	{
		GenericDataSource<InfiniteIntegerInputFormat> source = new GenericDataSource<InfiniteIntegerInputFormat>(
																		InfiniteIntegerInputFormat.class, "Source");
		MapContract mapper = new MapContract(DelayingMapper.class, source, "Delay Mapper");
		GenericDataSink sink = new GenericDataSink(DiscardingOutputFormat.class, mapper, "Sink");
		
		return new Plan(sink);
	}
	
	
	@Parameters
	public static Collection<Object[]> getConfigurations()
	{
		final ArrayList<Configuration> tConfigs = new ArrayList<Configuration>(); 
		tConfigs.add(new Configuration());
		return toParameterList(tConfigs);
	}
	
	
	public static final class DelayingMapper extends MapStub
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
}
