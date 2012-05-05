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

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.sdaa11.clustering.initial.InitialClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.main.MainClustering;
import eu.stratosphere.sopremo.sdaa11.clustering.treecreation.TreeCreator;

/**
 * Clustering without post-processing.<br>
 * Inputs:<br>
 * <ol>
 * <li>Sample points</li>
 * <li>Remaining points</li>
 * </ol>
 * Outputs:<br>
 * <ol>
 * <li>Assigned points</li>
 * <li>Cluster representations</li>
 * </ol>
 * 
 * 
 * @author skruse
 * 
 */
@InputCardinality(min = 2, max = 2)
@OutputCardinality(min = 2, max = 2)
public class SimpleClustering extends CompositeOperator<SimpleClustering> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5063867071090826368L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators()
	 */
	@Override
	public SopremoModule asElementaryOperators() {
		final SopremoModule module = new SopremoModule(this.getName(), 2, 2);

		final Source samplePointSource = module.getInput(0);
		final Source remainingPointsSource = module.getInput(1);

		final InitialClustering initialClustering = new InitialClustering()
				.withInputs(samplePointSource);

		final TreeCreator treeCreator = new TreeCreator()
				.withInputs(initialClustering);

		final MainClustering mainClustering = new MainClustering().withInputs(
				initialClustering, remainingPointsSource,
				treeCreator.getOutput(0), treeCreator.getOutput(1));
		
		module.getOutput(0).setInputs(mainClustering.getOutput(0));
		module.getOutput(1).setInputs(mainClustering.getOutput(1));

		return module;
	}

}
