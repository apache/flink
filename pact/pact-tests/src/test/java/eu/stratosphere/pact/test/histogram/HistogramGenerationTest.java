package eu.stratosphere.pact.test.histogram;

import java.io.IOException;
import java.net.InetAddress;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.runtime.histogram.AdaptiveSampleStub;
import eu.stratosphere.pact.runtime.histogram.HistogramStub;
import eu.stratosphere.pact.runtime.histogram.SortStub;
import eu.stratosphere.pact.runtime.histogram.data.IntegerInputFormat;
import eu.stratosphere.pact.runtime.histogram.data.IntegerOutputFormat;

public class HistogramGenerationTest implements PlanAssembler {

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		DataSourceContract<PactInteger, PactInteger> source = 
			new DataSourceContract<PactInteger,	PactInteger>(IntegerInputFormat.class, "hdfs://localhost:50090/smallZipf");
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> sample =
			new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(AdaptiveSampleStub.class);
		sample.setDegreeOfParallelism(1);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> sort =
			new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(SortStub.class);
		sort.setDegreeOfParallelism(1);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> histogram = 
			new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(HistogramStub.class);
		histogram.setDegreeOfParallelism(1);
		
		DataSinkContract<PactInteger, PactInteger> sink =
			new DataSinkContract<PactInteger, PactInteger>(IntegerOutputFormat.class, "hdfs://localhost:50090/histo");
		sink.setDegreeOfParallelism(1);
		
		sink.setInput(histogram);
		histogram.setInput(sort);
		sort.setInput(sample);
		sample.setInput(source);
		
		Plan p = new Plan(sink, "Histogram Test");
		
		return p;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		PlanAssembler test = new HistogramGenerationTest();
		Plan plan = test.getPlan();
		
		PactCompiler compiler = new PactCompiler(new DataStatistics(), new FixedSizeClusterCostEstimator());
		// perform the actual compilation
		OptimizedPlan optPlan = compiler.compile(plan);
		
		JobGraphGenerator codeGen = new JobGraphGenerator();
		JobGraph jobGraph = codeGen.compileJobGraph(optPlan);
		
		Configuration config = new Configuration();
		config.setString("jobmanager.rpc.address", InetAddress.getLocalHost().getHostAddress());
		
		JobClient client = new JobClient(jobGraph, config);
		client.submitJob();
	}

}
