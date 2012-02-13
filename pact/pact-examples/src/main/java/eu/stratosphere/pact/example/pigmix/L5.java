package eu.stratosphere.pact.example.pigmix;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class L5 implements PlanAssembler{
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
	
	
	public static class Join extends CoGroupStub {

		@Override
		public void coGroup(Iterator<PactRecord> records1,
				Iterator<PactRecord> records2, Collector out) {
			if(!records1.hasNext()){
				out.collect(records2.next());
			}
			
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String[])
	 */
	@Override
	public Plan getPlan(String... args)
	{
		final int parallelism = (args != null && args.length > 0) ? Integer.parseInt(args[0]) : 1;
		final String pageViewsFile = "hdfs://marrus.local:50040/user/pig/tests/data/pigmix/page_views";
		final String powerUsersFile = "hdfs://marrus.local:50040/user/pig/tests/data/pigmix/power_users";
		
		FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
		pageViews.setDegreeOfParallelism(parallelism);
		
		FileDataSource powerUsers = new FileDataSource(TextInputFormat.class, powerUsersFile, "Read PowerUsers");
		powerUsers.setDegreeOfParallelism(parallelism);
		
		MapContract projectPageViews = new MapContract(ProjectPageViews.class, pageViews, "Project Page Views");
		projectPageViews.setDegreeOfParallelism(parallelism);
		
		MapContract projectPowerUsers = new MapContract(ProjectPowerUsers.class, powerUsers, "Project Power Users");
		projectPowerUsers.setDegreeOfParallelism(parallelism);
		
		CoGroupContract joiner = new CoGroupContract(Join.class, PactString.class, 0, 0, projectPageViews, projectPowerUsers, "Join");
		joiner.setDegreeOfParallelism(parallelism);
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L5", joiner, "Result");
		sink.setDegreeOfParallelism(parallelism);
		sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 1);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);

		Plan plan = new Plan(sink, "L5 Anti Join wihth CoGroup");
		return plan;
	}
}
