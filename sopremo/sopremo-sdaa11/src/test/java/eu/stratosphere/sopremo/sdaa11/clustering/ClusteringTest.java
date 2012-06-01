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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.main.ClusterRest;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeCreator;
import eu.stratosphere.sopremo.sdaa11.clustering.util.Points;
import eu.stratosphere.sopremo.serialization.NaiveSchemaFactory;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 * 
 */
public class ClusteringTest {

	private int pointCount = 0;

	@Test
	public void testClustering() throws IOException {

		Clustering clustering = new Clustering();
		ElementarySopremoModule module = clustering.asElementaryOperators();
		module.inferSchema(new NaiveSchemaFactory());
		final SopremoTestPlan plan = new SopremoTestPlan(clustering);

		final List<IJsonNode> input1Nodes = Points
				.loadPoints(Points.POINTS1_PATH);
		for (final IJsonNode inputNode : input1Nodes)
			plan.getInput(0).add(inputNode);

		final List<IJsonNode> input2Nodes = Points
				.loadPoints(Points.POINTS2_PATH);
		for (final IJsonNode inputNode : input2Nodes)
			plan.getInput(1).add(inputNode);

		plan.run();

		for (final IJsonNode outputNode : plan.getActualOutput(0))
			System.out.println(outputNode);

	}

	private IJsonNode createPoint(final String... values) {
		return new Point("point" + this.pointCount++, Arrays.asList(values))
				.write(null);
	}

	@InputCardinality(min = 2, max = 2)
	public static class TestOperator extends CompositeOperator<TestOperator> {

		private static final long serialVersionUID = 1L;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
		 */
		@Override
		public ElementarySopremoModule asElementaryOperators() {
			final ElementarySopremoModule module = new ElementarySopremoModule(
					this.getName(), 2, 1);

			final Source sampleInput = module.getInput(0);
			final Source restInput = module.getInput(1);

			final Operator<?> sampleClustering = new InitialClustering()
					.withInputs(sampleInput);

			final Operator<?> treeCreator = new TreeCreator()
					.withInputs(sampleClustering.getOutput(0));

			final ClusterRest restClustering = new ClusterRest();
			restClustering.setInput(ClusterRest.SAMPLE_CLUSTERS_INPUT_INDEX,
					sampleClustering.getOutput(0));
			restClustering.setInput(ClusterRest.REST_POINTS_INPUT_INDEX,
					restInput);
			restClustering.setInput(ClusterRest.TREE_INPUT_INDEX,
					treeCreator.getOutput(0));

			module.getOutput(0).setInput(0, restClustering.getOutput(0));

			return module;
		}

	}

}
