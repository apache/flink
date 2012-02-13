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
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class L8 implements PlanAssembler{
	public static class ProjectPageViews extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
				
				rec.setField(0, new PactString("all"));
				if(!fields.get(2).getValue().isEmpty()){
					rec.setField(1, new PactInteger(Integer.parseInt(fields.get(2).getValue())));
					}else{
						rec.setField(1, new PactInteger(0));
					}
				if(!fields.get(6).getValue().isEmpty()){
					rec.setField(2, new PactDouble(Double.parseDouble(fields.get(6).getValue())));
					}else{
						rec.setField(2, new PactDouble(0));
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
				int timespent = 0;
				double revenue = 0;
				int nrevenue = 0;
		         while (records.hasNext()) {
		        	 rec = records.next();
		             timespent += rec.getField(1, PactInteger.class).getValue();
		             revenue += rec.getField(2, PactDouble.class).getValue();
		             nrevenue++;
		         }
		         PactRecord output = new PactRecord(new PactInteger(timespent), new PactDouble(revenue/nrevenue));
		         out.collect(output);
				
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

			
			MapContract projectPageViews = new MapContract(ProjectPageViews.class, pageViews, "Project Page Views");
			projectPageViews.setDegreeOfParallelism(parallelism);
			
			ReduceContract group = new ReduceContract(Group.class, PactString.class, 0, projectPageViews, "Group all");
			group.setDegreeOfParallelism(40);
			
			FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L8", group, "Result");
			sink.setDegreeOfParallelism(parallelism);
			sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactInteger.class);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactDouble.class);

			Plan plan = new Plan(sink, "L8 group all");
			return plan;
		}
}
