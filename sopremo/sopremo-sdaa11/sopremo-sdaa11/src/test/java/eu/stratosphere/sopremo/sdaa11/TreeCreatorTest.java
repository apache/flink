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
package eu.stratosphere.sopremo.sdaa11;

import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeCreator;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 *
 */
public class TreeCreatorTest {
	
	@Test
	public void testPreparation() {
		
		final TreeCreator treeCreator = new TreeCreator();
		
		final SopremoTestPlan plan = new SopremoTestPlan(treeCreator);
		
		IJsonNode p1 = new Point("p1", Arrays.asList("1", "2", "3")).write((IJsonNode) null);
		IJsonNode p2 = new Point("p2", Arrays.asList("2", "3")).write((IJsonNode) null);
		IJsonNode p3 = new Point("p3", Arrays.asList("1", "2", "3")).write((IJsonNode) null);
		IJsonNode p4 = new Point("p4", Arrays.asList("1", "2", "3")).write((IJsonNode) null);
		
		ArrayNode points1 = new ArrayNode(p1, p2);
		ArrayNode points2 = new ArrayNode(p3, p4);
		
		final ObjectNode clusterNode1 = new ObjectNode();
		clusterNode1.put("id", new TextNode("c1"));
		clusterNode1.put("clustroid", p1);
		clusterNode1.put("points", points1);
		
		final ObjectNode clusterNode2 = new ObjectNode();
		clusterNode2.put("id", new TextNode("c2"));
		clusterNode2.put("clustroid", p3);
		clusterNode2.put("points", points2);

		plan.getInput(0)
			.add(clusterNode1)
			.add(clusterNode2);

		plan.run();
		
		for (IJsonNode node : plan.getActualOutput(0)) {
			System.out.println(node);
		}
		
	}

}
