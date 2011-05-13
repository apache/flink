package eu.stratosphere.pact.testing;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Tests the fuzzy matching function of {@link TestPlan}.
 * 
 * @author Arvid Heise
 */
public class FuzzyTestPlanTest {
	/**
	 * Truncates a double after the first two decimal places.
	 * 
	 * @author Arvid Heise
	 */
	public static class DoubleTruncatingMap extends MapStub<Key, PactDouble, Key, PactDouble> {
		@Override
		public void map(Key key, PactDouble value, Collector<Key, PactDouble> out) {
			out.collect(key, new PactDouble((int) (value.getValue() * 100) / 100d));
		}
	};

	/**
	 * As no delta is set, this test plan should fail with truncation.
	 */
	@Test
	public void shouldFailInaccurateDoublesWithoutDelta() {
		final MapContract<Key, PactDouble, Key, PactDouble> map = new MapContract<Key, PactDouble, Key, PactDouble>(
			DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		TestPlanTest.assertTestRunFails(testPlan);
	}

	/**
	 * With set delta, this test plan should not fail despite the truncation.
	 */
	@Test
	public void shouldMatchInaccurateDoublesWithDelta() {
		final MapContract<Key, PactDouble, Key, PactDouble> map = new MapContract<Key, PactDouble, Key, PactDouble>(
			DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.setAllowedPactDoubleDelta(0.01);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.run();
	}

	/**
	 * With set delta, this test plan should not fail despite the truncation.
	 */
	@Test
	public void shouldMatchInaccurateDoublesWithDeltaAndSameKey() {
		final MapContract<Key, PactDouble, Key, PactDouble> map = new MapContract<Key, PactDouble, Key, PactDouble>(
			DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.setAllowedPactDoubleDelta(0.01);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(1), new PactDouble(2.3456));
		testPlan.getExpectedOutput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(1), new PactDouble(2.3456));
		testPlan.run();
	}
}
