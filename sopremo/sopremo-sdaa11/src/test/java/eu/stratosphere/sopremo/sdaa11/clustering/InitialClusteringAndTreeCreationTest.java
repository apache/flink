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
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeCreator;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 * 
 */
public class InitialClusteringAndTreeCreationTest {

	private int pointCount = 0;

	@Test
	public void testModerateDistribution() {

		final TestOperator testOperator = new TestOperator();
		final SopremoTestPlan plan = new SopremoTestPlan(testOperator);

		plan.getInput(0).add(this.createPoint("1", "2", "3", "4", "5"))
				.add(this.createPoint("1", "2", "3", "4"))
				.add(this.createPoint("1", "2", "4", "5"))
				.add(this.createPoint("a")).add(this.createPoint("b"))
				.add(this.createPoint("c")).add(this.createPoint("d"))
				.add(this.createPoint("e")).add(this.createPoint("f"))
				.add(this.createPoint("g")).add(this.createPoint("h"))
				.add(this.createPoint("i")).add(this.createPoint("j"))
				.add(this.createPoint("k")).add(this.createPoint("l"))
				.add(this.createPoint("m")).add(this.createPoint("n"))
				.add(this.createPoint("o")).add(this.createPoint("p"))
				.add(this.createPoint("q"));

		plan.run();

		for (final IJsonNode node : plan.getActualOutput(0))
			System.out.println(node);

	}

	private IJsonNode createPoint(final String... values) {
		return new Point("point" + this.pointCount++, Arrays.asList(values))
				.write(null);
	}

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
			final SopremoModule module = new SopremoModule(this.getName(), 1, 1);

			final Operator<?> input = module.getInput(0);
			final InitialClustering preparation = new InitialClustering()
					.withInputs(input);
			preparation.setMaxRadius(500);
			preparation.setMaxSize(100);
			final TreeCreator treeCreator = new TreeCreator()
					.withInputs(preparation);
			treeCreator.setTreeWidth(5);

			module.getOutput(0).setInput(0, treeCreator);

			return module.asElementary();
		}

	}

}
