package eu.stratosphere.pact.runtime.task;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.test.util.RegularlyGeneratedInputGenerator;
import eu.stratosphere.pact.runtime.test.util.TaskTestBase;

public class MapTaskTest extends TaskTestBase {

	List<KeyValuePair<PactInteger,PactInteger>> outList;
		
	@Test
	public void testMapTask() {

		int keyCnt = 100;
		int valCnt = 20;
		
		outList = new ArrayList<KeyValuePair<PactInteger,PactInteger>>();
		
		super.initEnvironment(1);
		super.addInput(new RegularlyGeneratedInputGenerator(keyCnt, valCnt));
		super.addOutput(outList);
		
		MapTask testTask = new MapTask();
		
		super.registerTask(testTask, MockMapStub.class);
		
		try {
			testTask.invoke();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue(outList.size() == keyCnt*valCnt);
		
	}
	
	public static class MockMapStub extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
			out.collect(key, value);
		}
		
	}
	
}
