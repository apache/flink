/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

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
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
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
		final MapContract map = new MapContract(DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.getExpectedOutput(PactInteger.class, PactDouble.class).
			add(new PactInteger(1), new PactDouble(1.2349)).
			add(new PactInteger(2), new PactDouble(2.3459));
		TestPlanTest.assertTestRunFails(testPlan);
	}

	/**
	 * With set delta, this test plan should not fail despite the truncation.
	 */
	@Test
	public void shouldMatchInaccurateDoublesWithDelta() {
		final MapContract map = new MapContract(DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.setAllowedPactDoubleDelta(0.01);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(2), new PactDouble(2.3456));
		testPlan.getExpectedOutput(PactInteger.class, PactDouble.class).
			add(new PactInteger(1), new PactDouble(1.2349)).
			add(new PactInteger(2), new PactDouble(2.3459));
		testPlan.run();
	}

	/**
	 * With set delta, this test plan should not fail despite the truncation.
	 */
	@Test
	public void shouldMatchInaccurateDoublesWithDeltaAndSameKey() {
		final MapContract map = new MapContract(DoubleTruncatingMap.class, "Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.setAllowedPactDoubleDelta(0.01);
		testPlan.getInput().
			add(new PactInteger(1), new PactDouble(1.2345)).
			add(new PactInteger(1), new PactDouble(2.3456));
		testPlan.getExpectedOutput(PactInteger.class, PactDouble.class).
			add(new PactInteger(1), new PactDouble(1.2349)).
			add(new PactInteger(1), new PactDouble(2.3459));
		testPlan.run();
	}
}
