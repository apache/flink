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
package eu.stratosphere.sopremo.sdaa11.clustering.main;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.tree.ClusterTree;
import eu.stratosphere.sopremo.sdaa11.clustering.util.Points;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 * 
 */
public class PointMapperTest {

	@Test
	public void testPointMapping() {

		final SopremoTestPlan plan = new SopremoTestPlan(new PointMapper());

		final ClusterTree tree = new ClusterTree(2);
		tree.add(new Point("p1", "a", "b", "c"), "c1");
		tree.add(new Point("p2", "1", "2", "3"), "c2");
		final IJsonNode treeNode = tree.write(null);
		System.out.println(treeNode);
		plan.getInput(1).add(treeNode);

		plan.getInput(0).add(Points.asJson("a", "b"))
				.add(Points.asJson("2", "3"));

		plan.run();

		for (final IJsonNode outputNode : plan.getActualOutput(0))
			System.out.println("Output: " + outputNode);
	}
}
