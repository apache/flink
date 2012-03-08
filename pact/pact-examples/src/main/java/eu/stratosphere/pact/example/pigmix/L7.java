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

public class L7 implements PlanAssembler{

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
				rec.setField(1, fields.get(5));
				out.collect(rec);
			}
		}	
	}
	 public static class Group extends ReduceStub{

			@Override
			public void reduce(Iterator<PactRecord> records, Collector out)
					throws Exception {
				PactRecord rec = null;
				int morning = 0;
				int afternoon = 0;
		         while (records.hasNext()) {
		        	 rec = records.next();
		             if (Integer.parseInt(rec.getField(1, PactString.class).getValue()) > 43200){
		            	 morning++;
		             }
		             else{
		            	 afternoon++;
		             }
		         }
		         PactRecord output = new PactRecord();
		         output.setNumFields(3);
		         output.setField(0, rec.getField(0, PactString.class));
		         output.setField(1, new PactInteger(morning));
		         output.setField(2, new PactInteger(afternoon));
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
			
			ReduceContract group = new ReduceContract(Group.class, PactString.class, 0, projectPageViews, "Group");
			group.setDegreeOfParallelism(40);
			
			FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L7", group, "Result");
			sink.setDegreeOfParallelism(parallelism);
			sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
			sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 2, PactInteger.class);
			
			Plan plan = new Plan(sink, "L7 nested plan with splits");
			return plan;
		}
}
