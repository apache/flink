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

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.ClusterToTreePreparation;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeAssembler;
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
		
		SopremoTestPlan plan = new SopremoTestPlan(new TestOperator());
		
		plan.getInput(0)
			.add(createPoint("1", "2", "3", "4", "5"))
			.add(createPoint("1", "2", "3", "4"))
			.add(createPoint("1", "2", "4", "5"))
			.add(createPoint("a"))
			.add(createPoint("b"))
			.add(createPoint("c"))
			.add(createPoint("d"))
			.add(createPoint("e"))
			.add(createPoint("f"))
			.add(createPoint("g"))
			.add(createPoint("h"))
			.add(createPoint("i"))
			.add(createPoint("j"))
			.add(createPoint("k"))
			.add(createPoint("l"))
			.add(createPoint("m"))
			.add(createPoint("n"))
			.add(createPoint("o"))
			.add(createPoint("p"))
			.add(createPoint("q"));
		
		plan.run();
		
		for (IJsonNode node : plan.getActualOutput(0)) {
			System.out.println(node);
		}
		
		
	}
	
	private IJsonNode createPoint(String... values) {
		return new Point("point"+(pointCount++), Arrays.asList(values)).write(null);
	}
	
	public static class TestOperator extends CompositeOperator<TestOperator> {

		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
		 */
		@Override
		public SopremoModule asElementaryOperators() {
			final SopremoModule module = new SopremoModule(this.getName(), 1, 1);

			final Operator<?> input = module.getInput(0);
			final InitialClustering preparation = new InitialClustering().withInputs(input);
			final TreeCreator treeCreator = new TreeCreator()
					.withInputs(preparation);

			module.getOutput(0).setInput(0, treeCreator);
		
			return module;
		}
		
	}

}
