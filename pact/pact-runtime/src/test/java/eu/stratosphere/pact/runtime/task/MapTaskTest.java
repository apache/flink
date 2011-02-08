package eu.stratosphere.pact.runtime.task;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

public class MapTaskTest extends TaskTestBase {

	MapTask testTask;
	
	int[] inKeys = {1,2,3,4,5,6,7,8,9};
	int[] inVals = {1,2,3,4,5,6,7,8,9};
	
	@Before
	public void prepare() {
		
		super.setupEnvironment();
		super.addInput(super.createInputIterator(inKeys, inVals));
		
		TaskConfig taskConfig = super.getTaskConfig();
		taskConfig.setStubClass(MockMapStub.class);
		taskConfig.addInputShipStrategy(ShipStrategy.FORWARD);

		testTask = new MapTask();
		super.registerTask(testTask);
		
	}
	
	@Test
	public void testMapTask() {

		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue(MockMapStub.cnt == 9);
		
	}
	
	@After
	public void cleanUp() {
		
	}
	
	public static class MockMapStub extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		public static int cnt = 0;
		
		@Override
		public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
			cnt++;
		}
		
	}
	
}
