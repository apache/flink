package eu.stratosphere.pact.example.pigmix;

import java.util.List;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class L9 implements PlanAssembler{
	public static class ProjectPageViews extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');


				rec.setField(0, fields.get(3));
				rec.setField(1, record.getField(0, PactString.class));
				out.collect(rec);
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

		FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
		pageViews.setDegreeOfParallelism(parallelism);


		MapContract projectPageViews = new MapContract(ProjectPageViews.class, pageViews, "Project Page Views");
		projectPageViews.setDegreeOfParallelism(parallelism);


		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L9", projectPageViews, "Result");
		sink.setDegreeOfParallelism(parallelism);
		sink.setGlobalOrder(Order.ASCENDING);
		sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactString.class);

		Plan plan = new Plan(sink, "L9 order by");
		return plan;
	}
}
