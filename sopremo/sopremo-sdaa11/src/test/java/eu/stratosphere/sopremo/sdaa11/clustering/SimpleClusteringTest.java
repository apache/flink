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
package eu.stratosphere.sopremo.sdaa11.clustering;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 *
 */
public class SimpleClusteringTest {
	
	private int pointCount = 0;
	
	@Test
	public void testClustering() {
		
//		SopremoTestPlan plan = new SopremoTestPlan(2, 1);
//		TestOperator testOperator = new TestOperator();
//		testOperator.setInputs(plan.getInputOperators(0, 2));
//		plan.getOutputOperator(0).setInput(0, testOperator);
		SopremoTestPlan plan = new SopremoTestPlan(new SimpleClustering());
		
		plan.getInput(0)
		.add(createPoint("sample_1"))
		.add(createPoint("sample_2"));
		
		plan.getInput(1)
		.add(this.createPoint("rest_1"));
		
		plan.run();
		
		for (IJsonNode outputNode: plan.getActualOutput(0))
			System.out.println(outputNode);
		
	}
	
	private IJsonNode createPoint(final String... values) {
		return new Point("point" + this.pointCount++, Arrays.asList(values))
				.write(null);
	}
	

}
