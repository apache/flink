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
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.DataStatistics.BasicFileStatistics;
import eu.stratosphere.pact.compiler.costs.FallbackCostEstimator;
import eu.stratosphere.pact.compiler.plan.DataSinkNode;
import eu.stratosphere.pact.compiler.plan.MapNode;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.ReduceNode;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.compiler.util.MockDataStatistics;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;


/**
 * Tests in this class:
 * <ul>
 *   <li>Tests that check the correct handling of the properties and strategies in the case where the degree of
 *       parallelism between tasks is increased or decreased.
 * </ul>
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class PropertiesPropagationTest {
	
	private static final String IN_FILE_1 = "file:///test/file";
	
	private static final String OUT_FILE_1 = "file///test/output";
	
	private static final int defalutParallelism = 8;
	
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
			MockDataStatistics dataStats = new MockDataStatistics();
			dataStats.setStatsForFile(IN_FILE_1, new BasicFileStatistics(1000, 128 * 1024 * 1024, 8.0f));
			
			this.compiler = new PactCompiler(dataStats, new FallbackCostEstimator(), dummyAddress);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test setup failed.");
		}
		
		// create the instance type description
		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, defalutParallelism * 2);
	}
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * 
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingDegreeOfParallelism()
	{
		final int degOfPar = defalutParallelism;
		
		// construct the plan
		DataSourceContract<PactInteger, PactInteger> source = new DataSourceContract<PactInteger, PactInteger>(DummyInputFormat.class, IN_FILE_1, "Source");
		source.setDegreeOfParallelism(degOfPar);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map1 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map1");
		map1.setDegreeOfParallelism(degOfPar);
		map1.setInput(source);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce1 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce 1");
		reduce1.setDegreeOfParallelism(degOfPar);
		reduce1.setInput(map1);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map2 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map2");
		map2.setDegreeOfParallelism(degOfPar * 2);
		map2.setInput(reduce1);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce2 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce 2");
		reduce2.setDegreeOfParallelism(degOfPar * 2);
		reduce2.setInput(map2);
		
		DataSinkContract<PactInteger, PactInteger> sink = new DataSinkContract<PactInteger, PactInteger>(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		DataSinkNode sinkNode = oPlan.getDataSinks().iterator().next();
		ReduceNode red2Node = (ReduceNode) sinkNode.getInputConnection().getSourcePact();
		MapNode map2Node = (MapNode) red2Node.getInputConnection().getSourcePact();
		
		Assert.assertEquals("The Reduce 1 Node has an invalid shipping strategy.", ShipStrategy.PARTITION_LOCAL_HASH, map2Node.getInputConnection().getShipStrategy());
	}
	
	@Test
	public void checkPropertyHandlingWithDecreasingDegreeOfParallelism()
	{
		final int degOfPar = defalutParallelism;
		
		// construct the plan
		DataSourceContract<PactInteger, PactInteger> source = new DataSourceContract<PactInteger, PactInteger>(DummyInputFormat.class, IN_FILE_1, "Source");
		source.setDegreeOfParallelism(degOfPar * 2);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map1 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map1");
		map1.setDegreeOfParallelism(degOfPar * 2);
		map1.setInput(source);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce1 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce 1");
		reduce1.setDegreeOfParallelism(degOfPar * 2);
		reduce1.setInput(map1);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map2 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map2");
		map2.setDegreeOfParallelism(degOfPar);
		map2.setInput(reduce1);
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce2 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce 2");
		reduce2.setDegreeOfParallelism(degOfPar);
		reduce2.setInput(map2);
		
		DataSinkContract<PactInteger, PactInteger> sink = new DataSinkContract<PactInteger, PactInteger>(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(degOfPar);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		DataSinkNode sinkNode = oPlan.getDataSinks().iterator().next();
		ReduceNode red2Node = (ReduceNode) sinkNode.getInputConnection().getSourcePact();
		
		Assert.assertEquals("The Reduce 2 Node has an invalid local strategy.", LocalStrategy.SORT, red2Node.getLocalStrategy());
	}

}
