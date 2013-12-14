package eu.stratosphere.pact.test.accumulators;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.accumulators.IntCounter;
import eu.stratosphere.api.operators.BulkIteration;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.test.util.TestBase2;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactString;
import eu.stratosphere.util.Collector;

/**
 * To be finished !!! Didn't test with iterations yet;-(
 */
@RunWith(Parameterized.class)
public class AccumulatorIterativeITCase extends TestBase2 {

	private static final String INPUT = "1\n" + "2\n" + "3\n";
	private static final String EXPECTED = "6\n";
	
	private static final int NUM_ITERATIONS = 3;
	private static final int NUM_SUBTASKS = 1;

	protected String dataPath;
	protected String resultPath;
	
	public AccumulatorIterativeITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("datapoints.txt", INPUT);
		resultPath = getTempFilePath("result");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
		
		Integer res = (Integer) getJobExecutionResult().getAccumulatorResult("test");
		Assert.assertEquals(new Integer(NUM_ITERATIONS * 6), res);
	}

	@Override
	protected Plan getPactPlan() {
		Plan plan = getTestPlanPlan(config.getInteger("IterationAllReducer#NoSubtasks", 1), dataPath, resultPath);
		return plan;
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("IterationAllReducer#NoSubtasks", NUM_SUBTASKS);
		return toParameterList(config1);
	}
	
	static Plan getTestPlanPlan(int numSubTasks, String input, String output) {

		FileDataSource initialInput = new FileDataSource(TextInputFormat.class, input, "input");
		
		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(initialInput);
		iteration.setMaximumNumberOfIterations(NUM_ITERATIONS);

		ReduceContract sumReduce = ReduceContract.builder(new SumReducer())
				.input(iteration.getPartialSolution())
				.name("Compute sum (Reduce)")
				.build();
		
		iteration.setNextPartialSolution(sumReduce);

		FileDataSink finalResult = new FileDataSink(RecordOutputFormat.class, output, iteration, "Output");
		RecordOutputFormat.configureRecordFormat(finalResult)
    		.recordDelimiter('\n')
    		.fieldDelimiter(' ')
    		.field(PactString.class, 0);

		Plan plan = new Plan(finalResult, "Iteration with AllReducer (keyless Reducer)");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
	
	static final class SumReducer extends ReduceStub implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		private IntCounter testCounter = new IntCounter();
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("test", this.testCounter);
		}
		
		@Override
		public void reduce(Iterator<PactRecord> it, Collector<PactRecord> out) {
			// Compute the sum
			int sum = 0;
			while (it.hasNext()) {
				Integer value = Integer.parseInt(it.next().getField(0, PactString.class).getValue());
				sum += value;
				testCounter.add(value);
			}
			out.collect(new PactRecord(new PactString(Integer.toString(sum))));
		}
	}

}
