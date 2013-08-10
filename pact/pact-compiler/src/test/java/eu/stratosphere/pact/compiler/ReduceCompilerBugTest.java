package eu.stratosphere.pact.compiler;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;


/**
 * This test case has been created to validate a bug that occurred when
 * the ReduceContract was used without a grouping key.
 */
public class ReduceCompilerBugTest extends CompilerTestBase  {

	@Test
	public void testReduce() {
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source");
		ReduceContract reduce1 = ReduceContract.builder(IdentityReduce.class).name("Reduce1").input(source).build();
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, "Sink");
		sink.setInput(reduce1);
		Plan plan = new Plan(sink, "Test Temp Task");
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		
		try {
			OptimizedPlan oPlan = compileNoStats(plan);
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly");
		}
	}
}
