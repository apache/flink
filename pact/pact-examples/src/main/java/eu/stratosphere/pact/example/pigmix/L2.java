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

// THIS CODE IS ADOPTED FROM THE ORIGINAL PIGMIX QUERY IMPLEMENTATIONS IN
// APACHE HADOOP'S MAP-REDUCE

package eu.stratosphere.pact.example.pigmix;

import java.util.List;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;


public class L2 implements PlanAssembler
{
	public static class ProjectPageViews extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
				
				rec.setField(0, fields.get(0));
				rec.setField(1, fields.get(6));
				out.collect(rec);
			}
		}	
	}
	
	public static class ProjectPowerUsers extends MapStub
	{
		private final PactRecord rec = new PactRecord();
		
		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
				rec.setField(0, fields.get(0));
				out.collect(rec);
			}
		}	
	}
	
	
	public static class Join extends MatchStub {
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector out) throws Exception
		{
			out.collect(value1);
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String[])
	 */
	@Override
	public Plan getPlan(String... args)
	{
		final int parallelism = (args != null && (args != null && args.length > 0)) ? Integer.parseInt(args[0]) : 1;
		final String pageViewsFile = "hdfs://marrus.local:50040/user/pig/tests/data/pigmix/page_views/";
		final String powerUsersFile = "hdfs://marrus.local:50040/user/pig/tests/data/pigmix/power_users/";
		
		FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
		pageViews.setDegreeOfParallelism(parallelism);
		
		FileDataSource powerUsers = new FileDataSource(TextInputFormat.class, powerUsersFile, "Read PowerUsers");
		powerUsers.setDegreeOfParallelism(parallelism);
		
		MapContract projectPageViews = new MapContract(ProjectPageViews.class, pageViews, "Project Page Views");
		projectPageViews.setDegreeOfParallelism(parallelism);
		
		MapContract projectPowerUsers = new MapContract(ProjectPowerUsers.class, powerUsers, "Project Power Users");
		projectPowerUsers.setDegreeOfParallelism(parallelism);
		
		MatchContract joiner = new MatchContract(Join.class, PactString.class, 0, 0, projectPageViews, projectPowerUsers, "Join");
		joiner.setDegreeOfParallelism(parallelism);
		joiner.setParameter("LOCAL_STRATEGY", "LOCAL_STRATEGY_HASH_BUILD_SECOND");
		joiner.setParameter("INPUT_RIGHT_SHIP_STRATEGY", "SHIP_BROADCAST");
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L2", joiner, "Result");
		sink.setDegreeOfParallelism(parallelism);
		sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);

		Plan plan = new Plan(sink, "L2 Broadcast Join");
		return plan;
	}
}
