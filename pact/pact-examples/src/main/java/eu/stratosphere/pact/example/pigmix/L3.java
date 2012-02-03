package eu.stratosphere.pact.example.pigmix;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class L3 implements PlanAssembler{
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
				rec.setField(1, new PactDouble(Double.parseDouble(fields.get(6).getValue())));
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
			out.collect(value2);
		}
	}
	 public static class Group extends ReduceStub{

			@Override
			public void reduce(Iterator<PactRecord> records, Collector out)
					throws Exception {
				PactRecord rec = null;
				 double cnt = 0;
		         while (records.hasNext()) {
		        	 rec = records.next();
		             cnt += rec.getField(1, PactDouble.class).getValue();
		         }
		         out.collect(new PactRecord(rec.getField(0, PactString.class), new PactDouble(cnt)));
				
			}
	 }
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.PlanAssembler#getPlan(java.lang.String[])
	 */
	@Override
	public Plan getPlan(String... args)
	{
		final int parallelism = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		final String pageViewsFile = "hdfs://cloud-7.dima.tu-berlin.de:40010/pigmix/pigmix625k/page_views";
		final String powerUsersFile = "hdfs://cloud-7.dima.tu-berlin.de:40010/pigmix/pigmix625k/power_users";
		
		FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
		pageViews.setDegreeOfParallelism(parallelism);
		
		FileDataSource powerUsers = new FileDataSource(TextInputFormat.class, powerUsersFile, "Read PowerUsers");
		powerUsers.setDegreeOfParallelism(parallelism);
		
		MapContract projectPageViews = new MapContract(ProjectPageViews.class, pageViews, "Project Page Views");
		projectPageViews.setDegreeOfParallelism(parallelism);
		
		MapContract projectPowerUsers = new MapContract(ProjectPowerUsers.class, powerUsers, "Project Power Users");
		projectPowerUsers.setDegreeOfParallelism(parallelism);
		
		MatchContract joiner = new MatchContract(Join.class, PactString.class, 0, 0, projectPowerUsers, projectPageViews,  "Join");
		joiner.setDegreeOfParallelism(parallelism);
		
		ReduceContract group = new ReduceContract(Group.class, PactString.class, 0, joiner, "Group");
		group.setDegreeOfParallelism(40);
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://cloud-7.dima.tu-berlin.de:40010/pigmix/result_L3", group, "Result");
		sink.setDegreeOfParallelism(parallelism);
		sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);

		Plan plan = new Plan(sink, "L3 Big Join with Grouping");
		return plan;
	}
}
