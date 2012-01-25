package eu.stratosphere.pact.testing;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
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
	public static class DoubleTruncatingMap extends MapStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector out) throws Exception {
			PactDouble value = record.getField(1, PactDouble.class);
			value.setValue((int) (value.getValue() * 100) / 100d);
			out.collect(record);
		}
	};

	/**
	 * As no delta is set, this test plan should fail with truncation.
	 */
	@Test
	public void shouldFailInaccurateDoublesWithoutDelta() {
		final MapContract map = new MapContract(			DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.getExpectedOutput(PactInteger.class, PactDouble.class).
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		TestPlanTest.assertTestRunFails(testPlan);
	}

	/**
	 * With set delta, this test plan should not fail despite the truncation.
	 */
	@Test
	public void shouldMatchInaccurateDoublesWithDelta() {
		final MapContract map = new MapContract(			DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.setAllowedPactDoubleDelta(0.01, 1);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.getExpectedOutput(PactInteger.class, PactDouble.class).
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.run();
	}

	/**
	 * With set delta, this test plan should not fail despite the truncation.
	 */
	@Test
	public void shouldMatchInaccurateDoublesWithDeltaAndSameKey() {
		final MapContract map = new MapContract(DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.setAllowedPactDoubleDelta(0.01, 1);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(1), new PactDouble(2.3456));
		testPlan.getExpectedOutput(PactInteger.class, PactDouble.class).
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(1), new PactDouble(2.3456));
		testPlan.run();
	}
}
