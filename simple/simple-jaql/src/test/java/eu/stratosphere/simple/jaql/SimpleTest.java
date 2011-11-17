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
package eu.stratosphere.simple.jaql;

import junit.framework.Assert;

import org.junit.Ignore;

import eu.stratosphere.simple.SimpleException;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoPlan;

/**
 * @author Arvid Heise
 */
@Ignore
public class SimpleTest {
	/**
	 * @param expectedPlan
	 * @param actualPlan
	 */
	protected static void assertEquals(SopremoPlan expectedPlan, SopremoPlan actualPlan) {
		Operator<?> unmatchingOperator = actualPlan.getUnmatchingOperator(expectedPlan);
		if (unmatchingOperator != null)
			Assert.failNotEquals(String.format("plans are different at %s", unmatchingOperator), expectedPlan,
				actualPlan);
	}

	public SopremoPlan parseScript(String script) {
		SopremoPlan plan = null;
		try {
			plan = new QueryParser().tryParse(script);
			System.out.println(new QueryParser().toJavaString(script));
		} catch (SimpleException e) {
			Assert.fail(String.format("could not parse script: %s", e.getMessage()));
		}

		Assert.assertNotNull("could not parse script", plan);

		System.out.println(plan);
		return plan;
	}
}
