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

import eu.stratosphere.sopremo.sdaa11.clustering.json.PointNodes;
import eu.stratosphere.sopremo.sdaa11.clustering.json.RepresentationNodes;
import eu.stratosphere.sopremo.sdaa11.util.JsonUtil2;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class RepresentationUpdateTest {

	@Test
	public void testRepresentationUpdate() {

		final SopremoTestPlan plan = new SopremoTestPlan(
				new RepresentationUpdate());

		final Input input0 = plan.getInput(0);
		input0.add(this.createRepresentationNode("c1", "p01", 0, "a", "b"));
		input0.add(this.createRepresentationNode("c2", "p02", 0, "1", "2"));

		final Input input1 = plan.getInput(1);
		input1.add(this.createAssignedPoint("p01", 0, "c1", "a", "b"));
		input1.add(this.createAssignedPoint("p02", 0, "c2", "1", "2"));
		input1.add(this.createAssignedPoint("p03", 0, "c1", "a", "b", "c"));
		input1.add(this.createAssignedPoint("p04", 0, "c1", "a", "c"));
		input1.add(this.createAssignedPoint("p05", 0, "c2", "1", "2", "3"));

		plan.run();

		for (final IJsonNode node : plan.getActualOutput(0))
			System.out.println(node);

	}

	private ObjectNode createAssignedPoint(final String key, final int rowsum,
			final String clusterId, final String... values) {
		final ObjectNode node = this.createPoint(key, rowsum, values);
		PointNodes.assignCluster(node, new TextNode(clusterId));
		return node;
	}

	private ObjectNode createPoint(final String key, final int rowsum,
			final String... values) {
		final ObjectNode node = new ObjectNode();
		final IArrayNode valuesNode = new ArrayNode();
		JsonUtil2.copyStrings(valuesNode, values);
		PointNodes.write(node, new TextNode(key), valuesNode, new IntNode(
				rowsum));
		return node;
	}

	private ObjectNode createRepresentationNode(final String clusterId,
			final String clustroidKey, final int clustroidRowsum,
			final String... clustroidValues) {
		final ObjectNode node = new ObjectNode();
		RepresentationNodes.write(node, new TextNode(clusterId), this
				.createPoint(clustroidKey, clustroidRowsum, clustroidValues));
		return node;
	}

}
