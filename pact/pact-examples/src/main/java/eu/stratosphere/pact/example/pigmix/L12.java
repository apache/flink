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


public class L12 implements PlanAssembler{
	
	public static class ProjectintoD extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
			
				if(fields.get(0).length() != 0 &&  fields.get(3).length() != 0){
					rec.setField(0, fields.get(0));
					rec.setField(1, fields.get(1));
					rec.setField(2, new PactInteger(Integer.parseInt(fields.get(2).getValue())));
					rec.setField(3, fields.get(3));
					rec.setField(4, new PactDouble(Double.parseDouble(fields.get(6).getValue())));
					out.collect(rec);
				}
				
			}
		}	
	}
	
	public static class HighestValuePagePerUser extends ReduceStub{

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
		throws Exception {
		    double max = 0;
		    PactRecord rec = new PactRecord();
            while (records.hasNext()) {
            	rec = records.next();
                double d = rec.getField(4, PactDouble.class).getValue();
                max = max > d ? max : d;
            }

            
			out.collect(new PactRecord(rec.getField(0, PactString.class), new PactDouble(max)));

		}
	}
	public static class ProjectintoAleph extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
			
				if(fields.get(0).length() != 0 &&  fields.get(3).length() == 0){
					rec.setField(0, fields.get(1));
					out.collect(rec);
				}
				
			}
		}	
	}
	
	public static class QueriesPerAction extends ReduceStub{

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
		throws Exception {
			int count = 0;
		    PactRecord rec = new PactRecord();
            while (records.hasNext()) {
            	rec = records.next();
                count ++;
            }
        
			out.collect(new PactRecord(rec.getField(0, PactString.class), new PactInteger(count)));

		}
	}
	public static class ProjectintoAlpha extends MapStub
	{
		private final PactRecord rec = new PactRecord();

		@Override
		public void map(PactRecord record, Collector out) throws Exception
		{
			PactString str = record.getField(0, PactString.class);
			if (str.length() > 0) {
				List<PactString> fields = Library.splitLine(str, '');
			
				if(fields.get(0).length() == 0 ){
					rec.setField(0, fields.get(3));
					rec.setField(1, new PactInteger(Integer.parseInt(fields.get(2).getValue())));
					out.collect(rec);
				}
				
			}
		}	
	}
	
	public static class TotalTimespentPerTerm extends ReduceStub{

		@Override
		public void reduce(Iterator<PactRecord> records, Collector out)
		throws Exception {
			double sum  = 0;
		    PactRecord rec = new PactRecord();
            while (records.hasNext()) {
            	rec = records.next();
                sum += rec.getField(1, PactDouble.class).getValue();
            }
        
			out.collect(new PactRecord(rec.getField(0, PactString.class), new PactDouble(sum)));

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
		FileDataSource pageViews = new FileDataSource(TextInputFormat.class, pageViewsFile, "Read PageViews");
		pageViews.setDegreeOfParallelism(parallelism);


		MapContract projectintoD = new MapContract(ProjectintoD.class, pageViews, "Project into D");
		projectintoD.setDegreeOfParallelism(parallelism);
		ReduceContract highestValuePagePerUser = new ReduceContract(HighestValuePagePerUser.class, PactString.class, 0, projectintoD, "HighestValuePagePerUser");
		highestValuePagePerUser.setDegreeOfParallelism(40);
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, "hdfs://cloud-7.dima.tu-berlin.de:40010/pigmix/result_L12_HighestValuePagePerUser", highestValuePagePerUser, "Result");
		sink.setDegreeOfParallelism(parallelism);
		sink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		sink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactDouble.class);
		
		MapContract projectintoaleph = new MapContract(ProjectintoAleph.class, pageViews, "Project into aleph");
		projectintoaleph.setDegreeOfParallelism(parallelism);
		ReduceContract queriesPerAction = new ReduceContract(QueriesPerAction.class, PactString.class, 0, projectintoaleph, "QueriesPerAction");
		queriesPerAction.setDegreeOfParallelism(40);
		FileDataSink querysink = new FileDataSink(RecordOutputFormat.class, "hdfs://cloud-7.dima.tu-berlin.de:40010/pigmix/result_L12_QueriesPerAction", queriesPerAction, "Result");
		querysink.setDegreeOfParallelism(parallelism);
		querysink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		querysink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		querysink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
		
		MapContract projectintoalpha = new MapContract(ProjectintoAlpha.class, pageViews, "Project into alpha");
		projectintoalpha.setDegreeOfParallelism(parallelism);
		ReduceContract  totalTimespentPerTerm = new ReduceContract(TotalTimespentPerTerm.class, PactString.class, 0, projectintoalpha, "TotalTimespentPerTerm");
		totalTimespentPerTerm.setDegreeOfParallelism(40);
		FileDataSink timespentsink = new FileDataSink(RecordOutputFormat.class, "hdfs://cloud-7.dima.tu-berlin.de:40010/pigmix/result_L12_TotalTimespentPerTerm", totalTimespentPerTerm, "Result");
		timespentsink.setDegreeOfParallelism(parallelism);
		timespentsink.getParameters().setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		timespentsink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactString.class);
		timespentsink.getParameters().setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactInteger.class);
		
		Plan plan = new Plan(sink, "L11 distinct, union and widerow");
		return plan;
	}
}
