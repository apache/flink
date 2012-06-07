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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.SON;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.util.Baskets;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 * 
 */
public class SONTest {

	@Test
	public void testSON() throws IOException {
		SON son = new SON();
		son.setParallelism(2);
		son.setMinSupport(10);
		son.setMaxSetSize(10);
		final SopremoTestPlan plan = new SopremoTestPlan(son);
		final List<IJsonNode> baskets = Baskets
				.loadBaskets(Baskets.BASKETS1_PATH);
		for (final IJsonNode basket : baskets)
			plan.getInput(0).add(basket);

		plan.run();

		for (final IJsonNode outputNode : plan.getActualOutput(0))
			System.out.println(outputNode);
	}

}
