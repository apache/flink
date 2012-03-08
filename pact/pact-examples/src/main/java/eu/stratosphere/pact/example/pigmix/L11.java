package eu.stratosphere.pact.example.pigmix;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;


public class L11 implements PlanAssembler{
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
	public static class ProjectWideRow extends MapStub
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
	
	
	public static class Distinct extends ReduceStub{

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
		throws Exception {

			out.collect(records.next());

		}
	}
	
	public static class UnionDistinct extends CoGroupStub {

		@Override
		public void coGroup(Iterator<PactRecord> records1,
				Iterator<PactRecord> records2, Collector out) {
			if(!records1.hasNext()){
				out.collect(records2.next());
			}else{
				out.collect(records1.next());
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
		final String wideRowFile = "hdfs://marrus.local:50040/user/pig/tests/data/pigmix/widerow";
		FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
		pageViews.setDegreeOfParallelism(parallelism);
		FileDataSource wideRow = new FileDataSource(TextInputFormat.class, wideRowFile, "Read WideRow");
		wideRow.setDegreeOfParallelism(parallelism);

		MapContract projectPageViews = new MapContract(ProjectPageViews.class, pageViews, "Project Page Views");
		projectPageViews.setDegreeOfParallelism(parallelism);
		MapContract projectWideRow = new MapContract(ProjectWideRow.class, pageViews, "Project Wide Row");
		projectWideRow.setDegreeOfParallelism(parallelism);
		
		ReduceContract distinctPageView = new ReduceContract(Distinct.class, PactString.class, 0, projectPageViews, "Distinct PageView");
		distinctPageView.setDegreeOfParallelism(40);
		ReduceContract distinctWideRow = new ReduceContract(Distinct.class, PactString.class, 0, projectWideRow, "Distinct Wide Row");
		distinctWideRow .setDegreeOfParallelism(40);
		
		CoGroupContract unionDistinct = new CoGroupContract(UnionDistinct.class, PactString.class, 0, 0, distinctPageView, distinctWideRow, "Join");
		unionDistinct.setDegreeOfParallelism(40);
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://marrus.local:50040/pigmix/result_L11", unionDistinct, "Result");
		sink.setDegreeOfParallelism(parallelism);
		sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 1);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);

		Plan plan = new Plan(sink, "L11 distinct, union and widerow");
		return plan;
	}
}
