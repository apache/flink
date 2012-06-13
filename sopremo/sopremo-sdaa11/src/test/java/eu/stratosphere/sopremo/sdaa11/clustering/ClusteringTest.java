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

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.main.ClusterRest;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeCreator;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 * 
 */
public class ClusteringTest {

	private int pointCount = 0;

	@Test
	public void testClustering() {

		// SopremoTestPlan plan = new SopremoTestPlan(2, 1);
		// TestOperator testOperator = new TestOperator();
		// testOperator.setInputs(plan.getInputOperators(0, 2));
		// plan.getOutputOperator(0).setInput(0, testOperator);
		final SopremoTestPlan plan = new SopremoTestPlan(new TestOperator());

		plan.getInput(0).add(this.createPoint("sample_1"))
				.add(this.createPoint("sample_2"));

		plan.getInput(1).add(this.createPoint("rest_1"));

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
