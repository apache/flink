package eu.stratosphere.pact.compiler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.DataStatistics.BasicFileStatistics;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.MapNode;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection.TempMode;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.MockDataStatistics;
import eu.stratosphere.pact.runtime.task.MapTask;
import eu.stratosphere.pact.runtime.task.TempTask;

/**
 * Tests in this class:
 * <ul>
 *   <li> Tests that temp task is successfully shared with instance of output task if sender side temp task
 *   <li> Tests that temp task is successfully shared with instance of input task if receiver side temp task
 * </ul>
 *
 * @author Moritz Kaufmann
 */
public class TempTaskSharingTest {	
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
			MockDataStatistics dataStats = new MockDataStatistics();
			dataStats.setStatsForFile(IN_FILE_1, new BasicFileStatistics(1000, 128 * 1024 * 1024, 8.0f));
			
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
	public void testTempTaskReceiverSideSharing()
	{
		JobGraph jobGraph = generateTempTaskJobGraph(TempMode.TEMP_RECEIVER_SIDE);
		
		//Get temp task vertex and receiver side vertex (map task)
		Iterator<JobTaskVertex> iter = jobGraph.getTaskVertices();
		JobTaskVertex mapVertex = null;
		JobTaskVertex tempVertex = null;
		while(iter.hasNext()) {
			JobTaskVertex nextVertex = iter.next();
			if(nextVertex.getTaskClass() == MapTask.class) {
				mapVertex = nextVertex;
			}
			else if(nextVertex.getTaskClass() == TempTask.class) {
				tempVertex = nextVertex;
			}
		}
		
		AbstractJobVertex sharedVertex = tempVertex.getVertexToShareInstancesWith();
		
		//Check if temp task is properly shared
		Assert.assertEquals(mapVertex, sharedVertex);
	}
	
	@Test
	public void testTempTaskSenderSideSharing()
	{
		JobGraph jobGraph = generateTempTaskJobGraph(TempMode.TEMP_SENDER_SIDE);
		
		//Get temp task vertex
		Iterator<JobTaskVertex> iter = jobGraph.getTaskVertices();
		JobTaskVertex tempVertex = null;
		while(iter.hasNext()) {
			JobTaskVertex nextVertex = iter.next();
			if(nextVertex.getTaskClass() == TempTask.class) {
				tempVertex = nextVertex;
			}
		}
		
		//Get sender side vertex (data source)
		AbstractJobVertex sourceVertex = jobGraph.getInputVertices().next();
		
		AbstractJobVertex sharedVertex = tempVertex.getVertexToShareInstancesWith();
		
		//Check if temp task is properly shared
		Assert.assertEquals(sourceVertex, sharedVertex);
	}
	
	private JobGraph generateTempTaskJobGraph(final TempMode mode) {
		// construct the plan
		DataSourceContract<PactInteger, PactInteger> source = new DataSourceContract<PactInteger, PactInteger>(DummyInputFormat.class, IN_FILE_1, "Source");
		source.setDegreeOfParallelism(defaultParallelism);
		
		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map1 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map1");
		map1.setDegreeOfParallelism(defaultParallelism);
		map1.setInput(source);
		
		DataSinkContract<PactInteger, PactInteger> sink = new DataSinkContract<PactInteger, PactInteger>(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(defaultParallelism);
		sink.setInput(map1);
		
		Plan plan = new Plan(sink, "Test Temp Task");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		//Inject temp strategy artificially
		oPlan.accept(new Visitor<OptimizerNode>() {
			@Override
			public boolean preVisit(OptimizerNode visitable) {
				if(visitable instanceof MapNode) {
					//Only OptimizerNode is MapContract
					visitable.getIncomingConnections().get(0).setTempMode(mode);
					return false;
				}
				return true;
			}
			
			@Override
			public void postVisit(OptimizerNode visitable) {
			}
		});
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		return jobGen.compileJobGraph(oPlan);
	}
}
