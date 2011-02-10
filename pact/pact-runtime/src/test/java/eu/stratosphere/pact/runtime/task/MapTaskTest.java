package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class MapTaskTest extends TaskTestBase {

	MapTask testTask;
	
	int[] inKeys = {1,2,3,4,5,6,7,8,9};
	int[] inVals = {1,2,3,4,5,6,7,8,9};
	
	List<KeyValuePair<PactInteger,PactInteger>> outList;
	
	@Before
	public void prepare() {
		
		outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();
		
		super.initEnvironment(1);
		super.addInput(super.createInputIterator(inKeys, inVals));
		super.addOutput(outList);
		
		testTask = new MapTask();
		
		super.registerTask(testTask, MockMapStub.class);
		
	}
	
	@Test
	public void testMapTask() {

		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue(outList.size() == 9);
		
	}
	
	@After
	public void cleanUp() {
		
	}
	
	public static class MockMapStub extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
			out.collect(key, value);
		}
		
	}
	
}
