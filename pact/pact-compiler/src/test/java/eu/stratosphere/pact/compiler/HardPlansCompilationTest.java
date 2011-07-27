package eu.stratosphere.pact.compiler;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKeyFirst;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;

/**
 * This class tests plans that once failed because of a bug:
 * <ul>
 *   <li> Ticket 158
 * </ul>
 *
 * @author Moritz Kaufmann
 */
public class HardPlansCompilationTest {	
	private static final String IN_FILE_1 = "file:///test/file";
	
	private static final String OUT_FILE_1 = "file///test/output";
	
	private static final int defaultParallelism = 8;
	
	// ------------------------------------------------------------------------
	
	private PactCompiler compiler;
	
	private InstanceTypeDescription instanceType;
	
	// ------------------------------------------------------------------------	
	
	@Before
	public void setup()
	{
		try {
			InetSocketAddress dummyAddress = new InetSocketAddress(InetAddress.getLocalHost(), 12345);
			
			// prepare the statistics
			DataStatistics dataStats = new DataStatistics();
			dataStats.cacheBaseStatistics(new TextInputFormat.FileBaseStatistics(1000, 128 * 1024 * 1024, 8.0f),
				FileDataSourceContract.getInputIdentifier(DummyInputFormat.class, IN_FILE_1));
			
			this.compiler = new PactCompiler(dataStats, new FixedSizeClusterCostEstimator(), dummyAddress);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test setup failed.");
		}
		
		// create the instance type description
		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, defaultParallelism * 2);
	}

	@Test
	/**
	 * Source -> Map -> Reduce -> Cross -> Reduce -> Cross -> Reduce ->
     * |--------------------------/                  /
     * |--------------------------------------------/
     * 
     * First cross has SameKeyFirst output contract
	 */
	public void testTicket158()
	{
		// construct the plan
		FileDataSourceContract<PactInteger, PactInteger> source = new FileDataSourceContract<PactInteger, PactInteger>(DummyInputFormat.class, IN_FILE_1, "Source");
		source.setDegreeOfParallelism(defaultParallelism);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map1");
		map.setDegreeOfParallelism(defaultParallelism);
		map.setInput(source);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce1 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce1");
		reduce1.setDegreeOfParallelism(defaultParallelism);
		reduce1.setInput(map);
		
		CrossContract<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> cross1 = new CrossContract<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger>(DummyCrossStub.class, "Cross1");
		cross1.setDegreeOfParallelism(defaultParallelism);
		cross1.setFirstInput(reduce1);
		cross1.setSecondInput(source);
		cross1.setOutputContract(SameKeyFirst.class);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce2 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce2");
		reduce2.setDegreeOfParallelism(defaultParallelism);
		reduce2.setInput(cross1);
		
		CrossContract<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> cross2 = new CrossContract<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger>(DummyCrossStub.class, "Cross2");
		cross2.setDegreeOfParallelism(defaultParallelism);
		cross2.setFirstInput(reduce2);
		cross2.setSecondInput(source);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce3 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce3");
		reduce3.setDegreeOfParallelism(defaultParallelism);
		reduce3.setInput(cross2);
		
		FileDataSinkContract<PactInteger, PactInteger> sink = new FileDataSinkContract<PactInteger, PactInteger>(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(defaultParallelism);
		sink.setInput(reduce3);
		
		Plan plan = new Plan(sink, "Test Temp Task");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		JobGraphGenerator jobGen = new JobGraphGenerator();
		jobGen.compileJobGraph(oPlan);
	}
}
