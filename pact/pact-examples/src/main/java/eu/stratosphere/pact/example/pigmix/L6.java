package eu.stratosphere.pact.example.pigmix;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class L6 implements PlanAssembler{
	public static class ProjectTimeSpent extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
				String key = fields.get(0).getValue() + ""+ fields.get(3).getValue() +""+ fields.get(4).getValue() + ""+fields.get(5).getValue();
				rec.setField(0,new PactString( key));
				if(!fields.get(2).getValue().isEmpty()){
				rec.setField(1, new PactInteger(Integer.parseInt(fields.get(2).getValue())));
				}else{
					rec.setField(1, new PactInteger(0));
				}
				out.collect(rec);
			}
		}	
	}
	 public static class Group extends ReduceStub{

			@Override
			public void reduce(Iterator<PactRecord> records, Collector out)
					throws Exception {
				PactRecord rec = null;
				int sum = 0;
		         while (records.hasNext()) {
		        	 rec = records.next();
		             sum += rec.getField(1, PactInteger.class).getValue();
		         }
		         out.collect(new PactRecord(rec.getField(0, PactString.class), new PactInteger(sum)));
				
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
			
			FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
			pageViews.setDegreeOfParallelism(parallelism);

			
			MapContract projectTimeSpent = new MapContract(ProjectTimeSpent.class, pageViews, "Project Time Spent");
			projectTimeSpent.setDegreeOfParallelism(parallelism);
			
			ReduceContract group = new ReduceContract(Group.class, PactString.class, 0, projectTimeSpent, "Group");
			group.setDegreeOfParallelism(40);
			
			FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L6", group, "Result");
			sink.setDegreeOfParallelism(parallelism);
			sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);

			Plan plan = new Plan(sink, "L6 group with long key");
			return plan;
		}
}
